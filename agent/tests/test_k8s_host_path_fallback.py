"""Regression tests for post-success host-path failures on in-cluster (k3s)
benches: Site() construction and fetch_latest_backup() both used to require
a host-mounted site checkout that a k3s pod-only site never gets (D6), even
though the underlying docker_execute() work already succeeded.
"""

from __future__ import annotations

import json
import os
import shutil
import unittest
from unittest.mock import patch

from agent.server import Server
from agent.site import Site
from agent.bench import Bench


class TestK8sHostPathFallback(unittest.TestCase):
    def _create_needed_paths(self, db_host: str):
        os.makedirs(self.sites_directory)
        os.makedirs(self.apps_directory)
        with open(self.common_site_config, "w") as c:
            json.dump({"db_host": db_host}, c)
        with open(self.bench_config, "w") as c:
            json.dump({"docker_image": "fake_img_url"}, c)
        with open(self.apps_txt, "w") as a:
            a.write("frappe\n")

    def setUp(self):
        self.test_dir = "test_k8s_host_path_fallback_dir"
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

        self.bench_name = "test-bench"
        self.benches_directory = os.path.join(self.test_dir, "benches")
        self.bench_dir = os.path.join(self.benches_directory, self.bench_name)

        self.sites_directory = os.path.join(self.bench_dir, "sites")
        self.apps_directory = os.path.join(self.bench_dir, "apps")
        self.common_site_config = os.path.join(self.sites_directory, "common_site_config.json")
        self.bench_config = os.path.join(self.bench_dir, "config.json")
        self.apps_txt = os.path.join(self.sites_directory, "apps.txt")

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def _get_bench(self, db_host: str) -> Bench:
        self._create_needed_paths(db_host)
        with patch.object(Server, "__init__", new=lambda x: None):
            server = Server()
        server.benches_directory = self.benches_directory
        return Bench(self.bench_name, server)

    # -- Site() construction after a successful pod-only new-site --

    def test_site_init_does_not_raise_when_host_checkout_missing_but_in_cluster(self):
        bench = self._get_bench(db_host="10.0.0.5")  # non-localhost -> in_cluster
        site_config = {"db_name": "mvp_db", "db_password": "secret"}

        with patch.object(Bench, "docker_execute", return_value={"output": json.dumps(site_config)}) as mock_exec:
            site = Site("mvp.local", bench)

        mock_exec.assert_called_once_with("cat site_config.json", subdir=os.path.join("sites", "mvp.local"))
        self.assertEqual(site.database, "mvp_db")
        self.assertEqual(site.password, "secret")

    def test_site_init_still_raises_when_host_checkout_missing_and_not_in_cluster(self):
        bench = self._get_bench(db_host="localhost")  # docker/host mode -> unchanged behaviour

        with self.assertRaises(OSError):
            Site("mvp.local", bench)

    # -- fetch_latest_backup() after a successful pod-only `bench backup` --

    def test_fetch_latest_backup_falls_back_to_pod_listing_when_host_backups_dir_missing(self):
        bench = self._get_bench(db_host="10.0.0.5")
        site_name = "backed-up.local"
        site_dir = os.path.join(self.sites_directory, site_name)
        os.makedirs(site_dir)
        with open(os.path.join(site_dir, "site_config.json"), "w") as f:
            json.dump({"db_name": "fake", "db_password": "fake"}, f)
        # Deliberately do NOT create <site_dir>/private/backups -- it only
        # exists inside the pod's PVC checkout.
        site = Site(site_name, bench)

        pod_files = [
            "20260101_000000-backed_up-database.sql.gz",
            "20260101_000000-backed_up-site_config_backup.json",
        ]

        def fake_docker_execute(command, subdir=None, **kwargs):
            if command.startswith("ls -1"):
                return {"output": "\n".join(pod_files)}
            if command.startswith("stat -c%s"):
                return {"output": "4096"}
            raise AssertionError(f"unexpected docker_execute call: {command!r}")

        with patch.object(Bench, "docker_execute", side_effect=fake_docker_execute):
            backups = site.fetch_latest_backup(with_files=False)

        self.assertEqual(backups["database"]["file"], pod_files[0])
        self.assertEqual(backups["database"]["size"], 4096)
        self.assertEqual(backups["site_config"]["file"], pod_files[1])

    def test_fetch_latest_backup_still_raises_when_backups_missing_and_not_in_cluster(self):
        bench = self._get_bench(db_host="localhost")
        site_name = "backed-up.local"
        site_dir = os.path.join(self.sites_directory, site_name)
        os.makedirs(site_dir)
        with open(os.path.join(site_dir, "site_config.json"), "w") as f:
            json.dump({"db_name": "fake", "db_password": "fake"}, f)
        site = Site(site_name, bench)

        with self.assertRaises(FileNotFoundError):
            site.fetch_latest_backup(with_files=False)

    def test_fetch_latest_backup_falls_back_when_host_dir_exists_but_has_no_matching_files(self):
        """Live repro (Backup Site job 9, 2026-08-26): the host backups
        directory exists (empty, or holding unrelated files) but the real
        backup only landed in the pod -- os.listdir() succeeds so the
        FileNotFoundError branch never fires, and max() on an empty list
        used to raise ValueError instead of falling back."""
        bench = self._get_bench(db_host="10.0.0.5")
        site_name = "backed-up.local"
        site_dir = os.path.join(self.sites_directory, site_name)
        os.makedirs(site_dir)
        with open(os.path.join(site_dir, "site_config.json"), "w") as f:
            json.dump({"db_name": "fake", "db_password": "fake"}, f)
        # Host backups dir exists, but empty -- no host-mounted PVC, D6.
        os.makedirs(os.path.join(site_dir, "private", "backups"))
        site = Site(site_name, bench)

        pod_files = [
            "20260101_000000-backed_up-database.sql.gz",
            "20260101_000000-backed_up-site_config_backup.json",
        ]

        def fake_docker_execute(command, subdir=None, **kwargs):
            if command.startswith("ls -1"):
                return {"output": "\n".join(pod_files)}
            if command.startswith("stat -c%s"):
                return {"output": "2048"}
            raise AssertionError(f"unexpected docker_execute call: {command!r}")

        with patch.object(Bench, "docker_execute", side_effect=fake_docker_execute):
            backups = site.fetch_latest_backup(with_files=False)

        self.assertEqual(backups["database"]["file"], pod_files[0])
        self.assertEqual(backups["database"]["size"], 2048)

    def test_fetch_latest_backup_still_raises_when_host_dir_empty_and_not_in_cluster(self):
        bench = self._get_bench(db_host="localhost")
        site_name = "backed-up.local"
        site_dir = os.path.join(self.sites_directory, site_name)
        os.makedirs(site_dir)
        with open(os.path.join(site_dir, "site_config.json"), "w") as f:
            json.dump({"db_name": "fake", "db_password": "fake"}, f)
        os.makedirs(os.path.join(site_dir, "private", "backups"))
        site = Site(site_name, bench)

        with self.assertRaises(ValueError):
            site.fetch_latest_backup(with_files=False)

    # -- update_config()/get_config()/set_config() after a successful
    #    pod-only new-site (Site() no longer OSError's, per the fix above,
    #    but the config read/write still hit the host) --

    def test_update_config_reads_and_writes_via_pod_when_host_checkout_missing(self):
        bench = self._get_bench(db_host="10.0.0.5")
        site_config = {"db_name": "loop4_db", "db_password": "secret"}

        with patch.object(Bench, "docker_execute", return_value={"output": json.dumps(site_config)}):
            site = Site("loop4.local", bench)

        written = {}

        def fake_docker_execute(command, subdir=None, input=None, **kwargs):
            if command == "cat site_config.json":
                return {"output": json.dumps(site_config)}
            if command == "cat > site_config.json":
                written["subdir"] = subdir
                written["value"] = json.loads(input)
                return {"output": ""}
            raise AssertionError(f"unexpected docker_execute call: {command!r}")

        with patch.object(Bench, "docker_execute", side_effect=fake_docker_execute):
            Site.update_config.__wrapped__(site, {"host_name": "https://loop4.local"})

        self.assertEqual(written["subdir"], os.path.join("sites", "loop4.local"))
        self.assertEqual(written["value"]["db_name"], "loop4_db")
        self.assertEqual(written["value"]["host_name"], "https://loop4.local")

    def test_update_config_still_uses_host_file_when_not_in_cluster(self):
        bench = self._get_bench(db_host="localhost")
        site_name = "host-mode.local"
        self._create_test_site(site_name)
        site = Site(site_name, bench)

        Site.update_config.__wrapped__(site, {"host_name": "https://host-mode.local"})

        with open(site.config_file) as f:
            saved = json.load(f)
        self.assertEqual(saved["host_name"], "https://host-mode.local")

    def _create_test_site(self, site_name: str):
        site_dir = os.path.join(self.sites_directory, site_name)
        os.makedirs(site_dir)
        with open(os.path.join(site_dir, "site_config.json"), "w") as f:
            json.dump({"db_name": "fake", "db_password": "fake"}, f)

    # -- new_site() should not touch host nginx when in_cluster (D8): the
    #    pod's `bench new-site` already succeeded, but host nginx isn't the
    #    k3s ingress (Traefik is), and `sudo systemctl reload nginx` would
    #    fail the job after the real work is done. --

    def test_new_site_skips_host_nginx_when_in_cluster(self):
        bench = self._get_bench(db_host="10.0.0.5")  # non-localhost -> in_cluster
        site_config = {"db_name": "loop6_db", "db_password": "secret"}

        with (
            patch.object(Bench, "bench_new_site"),
            patch.object(Site, "install_apps"),
            patch.object(Site, "update_config"),
            patch.object(Site, "enable_scheduler"),
            patch.object(Bench, "docker_execute", return_value={"output": json.dumps(site_config)}),
            patch.object(Bench, "setup_nginx") as mock_setup_nginx,
            patch.object(bench.server, "reload_nginx", create=True) as mock_reload_nginx,
        ):
            Bench.new_site.__wrapped__(bench, "loop6.local", {}, [], "root-pw", "admin-pw")

        mock_setup_nginx.assert_not_called()
        mock_reload_nginx.assert_not_called()

    def test_new_site_still_reloads_host_nginx_when_not_in_cluster(self):
        bench = self._get_bench(db_host="localhost")  # docker/host mode -> unchanged behaviour
        site_name = "host-mode-new.local"
        self._create_test_site(site_name)

        with (
            patch.object(Bench, "bench_new_site"),
            patch.object(Site, "install_apps"),
            patch.object(Site, "update_config"),
            patch.object(Site, "enable_scheduler"),
            patch.object(Bench, "setup_nginx") as mock_setup_nginx,
            patch.object(bench.server, "reload_nginx", create=True) as mock_reload_nginx,
        ):
            Bench.new_site.__wrapped__(bench, site_name, {}, [], "root-pw", "admin-pw")

        mock_setup_nginx.assert_called_once()
        mock_reload_nginx.assert_called_once()


if __name__ == "__main__":
    unittest.main()
