"""End-to-end series for the k3s MVP jobs already proven live against a real
cluster (kagent f14e0d8: New Site 14, Migrate 13, Backup 11, Restore 15, all
Success). This test re-runs the same four jobs, in the same order, against an
in_cluster (db_host != localhost) Bench with Bench.docker_execute mocked out,
so the series is fast/deterministic and needs no real cluster.

Each job is invoked via its `.__wrapped__` attribute -- the @job decorator's
underlying function -- so the job runs synchronously in-process instead of
enqueuing through RQ/sqlite (see agent/job.py).
"""

from __future__ import annotations

import json
import os
import shutil
import unittest
from unittest.mock import patch

from agent.bench import Bench
from agent.server import Server
from agent.site import Site


class TestK3sMvpEndToEnd(unittest.TestCase):
    """New Site -> Migrate -> Backup -> Restore, run as one ordered series
    against a single in-cluster Bench/Site, mirroring the live job order."""

    SITE_NAME = "mvp-e2e.local"

    def setUp(self):
        self.test_dir = "test_k3s_mvp_e2e_dir"
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

        os.makedirs(self.sites_directory)
        os.makedirs(self.apps_directory)
        with open(self.common_site_config, "w") as c:
            json.dump({"db_host": "10.0.0.5"}, c)  # non-localhost -> in_cluster
        with open(self.bench_config, "w") as c:
            json.dump({"docker_image": "fake_img_url"}, c)
        with open(self.apps_txt, "w") as a:
            a.write("frappe\n")

        with patch.object(Server, "__init__", new=lambda x: None):
            server = Server()
        server.benches_directory = self.benches_directory
        self.bench = Bench(self.bench_name, server)
        self.assertTrue(self.bench.in_cluster)

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def _site_config_payload(self):
        return {"db_name": "mvp_e2e_db", "db_password": "secret"}

    # -- step 1: New Site --

    def test_series_new_site_then_migrate_then_backup_then_restore(self):
        bench = self.bench
        site_config = self._site_config_payload()

        # 1. New Site: bench_new_site + everything else happens via
        # docker_execute()/pod-relative reads; host nginx must stay untouched
        # (D8) since Traefik, not nginx, fronts k3s.
        with (
            patch.object(Bench, "bench_new_site") as mock_bench_new_site,
            patch.object(Site, "install_apps") as mock_install_apps,
            patch.object(Site, "update_config") as mock_update_config,
            patch.object(Site, "enable_scheduler") as mock_enable_scheduler,
            patch.object(Bench, "docker_execute", return_value={"output": json.dumps(site_config)}),
            patch.object(Bench, "setup_nginx") as mock_setup_nginx,
            patch.object(bench.server, "reload_nginx", create=True) as mock_reload_nginx,
        ):
            Bench.new_site.__wrapped__(
                bench, self.SITE_NAME, {}, [], "root-pw", "admin-pw", create_user=None
            )

        mock_bench_new_site.assert_called_once()
        mock_install_apps.assert_called_once()
        mock_update_config.assert_called_once()
        mock_enable_scheduler.assert_called_once()
        mock_setup_nginx.assert_not_called()
        mock_reload_nginx.assert_not_called()

        # Site() construction must not raise even though there's no
        # host-mounted checkout for this pod-only site (D6).
        with patch.object(Bench, "docker_execute", return_value={"output": json.dumps(site_config)}):
            site = Site(self.SITE_NAME, bench)
        self.assertEqual(site.database, "mvp_e2e_db")

        # 2. Migrate: @step wrappers need a live Job record; stub the steps
        # and still require migrate_job itself to run in series order.
        with (
            patch.object(Site, "migrate") as mock_migrate,
            patch.object(Site, "disable_maintenance_mode") as mock_disable_mm,
        ):
            Site.migrate_job.__wrapped__(site, skip_failing_patches=False, activate=True)

        mock_migrate.assert_called_once_with(skip_failing_patches=False)
        mock_disable_mm.assert_called_once()

        # 3. Backup: @step `backup()` needs a Job record; stub it and still
        # run backup_job in series (offsite empty on k3s MVP).
        backup_files = [
            "20260101_000000-mvp_e2e-database.sql.gz",
            "20260101_000000-mvp_e2e-site_config_backup.json",
        ]
        backups = {
            "database": {"file": backup_files[0], "size": 1024},
            "site_config": {"file": backup_files[1], "size": 1024},
        }
        with (
            patch.object(Site, "backup_encryption_enabled", new=False),
            patch.object(Site, "backup", return_value=backups),
        ):
            backup_result = Site.backup_job.__wrapped__(site, with_files=False)

        self.assertEqual(backup_result["backups"]["database"]["file"], backup_files[0])
        self.assertEqual(backup_result["offsite"], {})

        # 4. Restore: the backup files already live in the pod's own PVC, so
        # restore_job() must skip the host download/upload round-trip (D6)
        # and pass pod-relative paths straight to `bench restore`.
        database_url = f"https://{self.SITE_NAME}/backups/db.sql.gz"
        public_url = f"https://{self.SITE_NAME}/backups/files.tar"
        private_url = f"https://{self.SITE_NAME}/backups/private-files.tar"

        def fake_docker_execute_restore(command, subdir=None, **kwargs):
            if command.startswith("test -f"):
                return {"returncode": 0}
            raise AssertionError(f"unexpected docker_execute call: {command!r}")

        with (
            patch.object(Bench, "docker_execute", side_effect=fake_docker_execute_restore),
            patch.object(Bench, "download_files") as mock_download_files,
            patch.object(Bench, "delete_downloaded_files") as mock_delete_downloaded_files,
            patch.object(Site, "restore_site") as mock_restore_site,
            patch.object(Site, "uninstall_unavailable_apps"),
            patch.object(Site, "migrate"),
            patch.object(Site, "set_admin_password"),
            patch.object(Site, "enable_scheduler"),
            patch.object(Site, "bench_execute", return_value={"output": ""}),
            patch.object(Bench, "setup_nginx") as mock_setup_nginx_restore,
            patch.object(bench.server, "reload_nginx", create=True) as mock_reload_nginx_restore,
        ):
            mock_download_files.return_value = {
                "directory": "/tmp/unused",
                "database": "",
                "public": "",
                "private": "",
            }

            Site.restore_job.__wrapped__(
                site, [], "root-pw", "admin-pw", database_url, public_url, private_url, None, False
            )

        mock_download_files.assert_called_once_with(self.SITE_NAME, None, None, None)
        mock_delete_downloaded_files.assert_called_once()
        mock_restore_site.assert_called_once()
        _, _, database_file, public_file, private_file = mock_restore_site.call_args.args
        self.assertEqual(database_file, f"sites/{self.SITE_NAME}/private/backups/db.sql.gz")
        self.assertEqual(public_file, f"sites/{self.SITE_NAME}/private/backups/files.tar")
        self.assertEqual(private_file, f"sites/{self.SITE_NAME}/private/backups/private-files.tar")

        # in_cluster restore must not touch host nginx either (D8).
        mock_setup_nginx_restore.assert_not_called()
        mock_reload_nginx_restore.assert_not_called()


if __name__ == "__main__":
    unittest.main()
