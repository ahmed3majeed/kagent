# Kagent — Change Analysis

Docker→Kubernetes (k3s) migration analysis of `frappe/agent`, read directly from
`/home/frappe/kagent/agent/` on 92.5.91.195. Line numbers are taken from real
`grep -n`/`cat -n` output against the cloned source, not estimated.

## Summary
Total files to change: 4
Total items to change: 88
Total items unchanged: 25

Two of the six files reviewed (`proxy.py`, `database_server.py`) need **zero** changes —
confirmed via `grep -n -i docker` returning no matches in either file.

| File | Lines | Imports `docker` SDK | HIGH | MEDIUM | LOW | Total |
|---|---|---|---|---|---|---|
| bench.py | 1647 | No (via `docker_execute()`/shell) | 38 | 5 | 0 | 43 |
| server.py | 1328 | No (shell `docker` CLI) | 20 | 1 | 1 | 22 |
| site.py | 1847 | No (via `bench.docker_execute()`) | 8 | 2 | 1 | 11 |
| proxy.py | 498 | No — zero Docker references | 0 | 0 | 0 | 0 |
| database_server.py | 1281 | No — zero Docker references | 0 | 0 | 0 | 0 |
| builder.py | 942 | **Yes** — `import docker`, `.from_env()`, `.images.push()` | 10 | 0 | 2 | 12 |
| **Total** | | | **76** | **8** | **4** | **88** |

### Architectural gaps (not mechanical renames)

These six items need a real design decision, not a `docker_execute()` → `k8s_execute()`
rename — each is called out again inline at its line number below, but is worth stating
up front since they drive the shape of the whole migration:

1. **`docker commit` has no Kubernetes/containerd equivalent.** `bench.py`
   `commit_container_changes()` (line 1234) and `builder.py` `_commit_patch_image()`
   (line 893) both snapshot a live container's filesystem into a new image. "Update in
   place" and "patch build" need to become real rebuilds instead.
2. **`docker run -d --name X` / `docker stack deploy` single-host lifecycle** (`bench.py`
   `start()`/`stop()`, lines 822–874) needs full redesign around K8s
   Deployment/Service/PVC objects, including translating the port-mapping scheme
   (web/socketio/codeserver/rq/ssh, several bound to `127.0.0.1`) to a Service/Ingress
   model.
3. **`docker update --memory/--cpus`** (`bench.py` `_update_runtime_limits()`, line 899)
   maps to `kubectl patch` on a Deployment's resources — but that triggers a pod
   restart, unlike Docker's live in-place update. Different operational semantics, not
   just a different command.
4. **`docker buildx build`** (`builder.py` `_get_build_command()`, line 625) — decide
   whether builds stay on dedicated Docker/BuildKit hosts outside the cluster
   (simplest) or move to an in-cluster builder like kaniko (bigger lift, but removes the
   `import docker` SDK dependency in #5 too).
5. **Docker SDK registry push** (`builder.py` `client.images.push()`, lines 709/908)
   needs a replacement regardless of the builds decision — it's the one hard
   Python-level `docker` SDK dependency across all six files.
6. **`docker exec` orphan-process cleanup assumption** in `site.py`'s streaming-backup
   path (`terminate_in_container_backup()`, line 1154) needs verification against
   `kubectl exec`'s actual process-orphaning behavior under containerd/k3s before
   relying on the same `pkill`-based cleanup pattern.

Also worth confirming with stakeholders explicitly: `database_server.py`'s MariaDB tier
and `proxy.py`'s nginx tier appear to be *intentionally* staying outside the K8s
migration scope — both files are simply silent on Docker because they never touch it,
which isn't the same as a confirmed architectural decision.

---

## bench.py
1647 lines. No direct `import docker` — all container interaction goes through the
internal `docker_execute()` method (definition below) or raw `execute()` shell calls.

### Must change:
- [ ] [HIGH] line 61: `self.docker_image = self.bench_config.get("docker_image")`
      → becomes a K8s container image reference read from bench config; field name/semantics likely kept, consumer logic changes
- [ ] [HIGH] line 177: `docker_execute()` definition — branches on `single_container` (`docker exec`) vs. Swarm multi-container (`docker service ps` + `docker exec` into the resolved task)
      → replace with `k8s_execute()` using `kubectl exec`; needs a pod-discovery step equivalent to the `docker service ps` task lookup (e.g. `kubectl get pods -l app={name}`)
- [ ] [HIGH] line 206: `docker_execute()` call in `bench_new_site` (bench new-site)
      → call site only, follows the line 177 rename
- [ ] [HIGH] line 347: `self.execute(f"docker top {self.name} | grep gunicorn")` in `get_worker_pids()`
      → `docker top` has no direct kubectl equivalent; needs `kubectl exec ... ps` or `kubectl top pod`
- [ ] [HIGH] line 449: `docker_execute()` call in `bench_archive_site` (bench drop-site)
      → call site only
- [ ] [MEDIUM] line 571: `if "No such container" in e.data["output"]:` in `disable_production()`
      → Docker-specific error string match; must match kubectl's not-found error text, or be replaced with a proper existence check
- [ ] [HIGH] line 582: `docker_execute(f"bench restart ...")` in `restart()`
      → call site only
- [ ] [HIGH] line 591: `docker_execute("bench build")` in `rebuild()`
      → call site only
- [ ] [HIGH] line 594: `docker_execute(f"bench build --app {apps[0]}")` in `rebuild()`
      → call site only
- [ ] [HIGH] line 596: `docker_execute(f"bench build --apps {...}")` in `rebuild()`
      → call site only
- [ ] [MEDIUM] line 693: `self.generate_docker_compose_file()` invoked from `update_config_job` (non-single_container path)
      → tied to Swarm stack deploy; needs to become a K8s manifest generation + `kubectl apply` path
- [ ] [HIGH] line 699: `docker_execute("supervisorctl reread")` in `update_supervisor()`
      → call site only
- [ ] [HIGH] line 700: `docker_execute("supervisorctl update")` in `update_supervisor()`
      → call site only
- [ ] [MEDIUM] lines 735-740: `generate_docker_compose_file()` — renders `docker-compose.yml.jinja2`, consumed by `docker stack deploy`
      → replace entirely with K8s manifest templates (Deployment/Service/PVC yaml)
- [ ] [HIGH] line 762: `docker_execute("supervisorctl start code-server:")` in `_start_code_server()`
      → call site only
- [ ] [HIGH] lines 764-766: `docker_execute(sed ... config.yaml)` in `_start_code_server()`
      → call site only
- [ ] [HIGH] line 767: `docker_execute("supervisorctl restart code-server:")` in `_start_code_server()`
      → call site only
- [ ] [HIGH] line 771: `docker_execute("supervisorctl stop code-server:")` in `_stop_code_server()`
      → call site only
- [ ] [HIGH] line 792: `docker_execute("supervisorctl stop code-server:")` in `remove_code_server()`
      → call site only
- [ ] [MEDIUM] lines 794-820: `prepare_mounts_on_host()` — builds Docker `-v host:container` mount flag strings from `self.mounts`
      → must become K8s volume/volumeMount + PVC (or hostPath) spec generation
- [ ] [HIGH] lines 822-868: `start()` — single-container path issues `docker stop`/`rm` then a `docker run -d ...` with full port map + volume mounts; multi-container path issues `docker stack deploy`
      → architectural gap #2: replace entirely with K8s Deployment + Service + PVC/volumeMount creation via `kubectl apply`/client-go
- [ ] [HIGH] lines 870-874: `stop()` — `docker rm {name} --force` (single) / `docker stack rm {name}` (multi)
      → `kubectl delete deployment/service` (or scale to 0)
- [ ] [HIGH] line 878: `_stop()` — `docker stop {name}`
      → `kubectl scale deployment {name} --replicas=0` or delete pod
- [ ] [HIGH] line 882: `_start()` — `docker start {name}`
      → `kubectl scale deployment {name} --replicas=1`; note semantic gap — K8s pods are replaced, not "restarted in place"
- [ ] [MEDIUM] lines 890-897: `update_runtime_limits()` — orchestration wrapper deciding whether limits need updating
      → logic itself is runtime-agnostic, but computed values (memory_high/memory_max/memory_swap/vcpu) map differently onto K8s requests/limits
- [ ] [HIGH] lines 899-915: `_update_runtime_limits()` — `docker update {name} --memory-reservation=... --memory=... --cpus=...`
      → architectural gap #3: `kubectl patch deployment` on resources; triggers a pod restart, unlike Docker's live update
- [ ] [HIGH] line 1056: `docker_execute(command, subdir=app_path)` in `git_apply()`
      → call site only
- [ ] [HIGH] line 1067: `docker_execute(f"supervisorctl {command} {target}")` in `run_supervisorctl_command()`
      → call site only
- [ ] [HIGH] line 1126: `exec = partial(self.docker_execute, subdir=app_path)` in `_pull_app_change()`
      → becomes a `k8s_execute` partial
- [ ] [HIGH] line 1132: `exec("git rev-parse ...")` via the partial above
      → call site only, follows line 1126
- [ ] [HIGH] line 1136: `exec("git remote remove ...")` via the partial above
      → call site only
- [ ] [HIGH] line 1140: `exec("git fetch ...")` via the partial above
      → call site only
- [ ] [HIGH] line 1141: `exec("git diff ...")` via the partial above
      → call site only
- [ ] [HIGH] line 1144: `exec("git reset --hard ...")` via the partial above
      → call site only
- [ ] [HIGH] line 1145: `exec("git clean -fd")` via the partial above
      → call site only
- [ ] [HIGH] line 1146: `exec("git checkout ...")` via the partial above
      → call site only
- [ ] [HIGH] line 1149: `exec("git remote remove ...")` via the partial above
      → call site only
- [ ] [HIGH] lines 1159-1174: `docker_execute(f"git remote get-url/remove/add ...")` in `set_git_remote()` (lines 1159, 1169, 1174)
      → call sites only
- [ ] [HIGH] line 1189: `docker_execute("bench setup requirements" + flag)` in `setup_requirements()`
      → call site only
- [ ] [HIGH] lines 1234-1239: `commit_container_changes()` — `docker ps -aqf "name={self.name}"` then `docker commit {container_id} {image}` to snapshot a live container into a new image, then updates `docker_image` in bench config
      → architectural gap #1: **no direct K8s equivalent**; "update in place" needs redesign around proper rebuilds through `builder.py`'s pipeline, or a snapshot/sidecar mechanism
- [ ] [HIGH] line 1399: `bench.docker_execute("bench doctor")` in module-level `_inactive_scheduler_sites()`
      → call site only

### Stays unchanged:
- `new_site`, `new_site_from_backup`, `archive_site`, `rename_site_job` — bench lifecycle orchestration jobs operate on Frappe/bench semantics, not the container runtime
- `migrate_sites`, `patch_app` — call through `docker_execute()` but encode no Docker-specific behavior themselves
- `update_inplace`'s decision logic (`get_should_run_update_phase`, `should_*` helpers) — pure Frappe-version/state logic
- `_sites()`, `apps` property — filesystem/config reads
- `generate_nginx_config`/`setup_nginx` — nginx config generation, not container-runtime dependent
- `bench_config`/`common_site_config`/`set_bench_config` and the CORS/hostname normalization helpers, plus MariaDB user create/drop via plain `mysql -h ...` CLI — all host-filesystem or direct-DB operations

---

## server.py
1328 lines. No direct `import docker` — uses shell-out `docker` CLI via `self.execute()`,
plus imports `parse_docker_df_output` (a parsing helper, not the SDK) from
`agent.application_storage_analyzer` — a file outside this six-file scope.

### Must change:
- [ ] [LOW] line 23: `parse_docker_df_output` import from `agent.application_storage_analyzer`
      → flag for cross-file follow-up; `docker system df` won't exist under k3s' containerd runtime, so the storage-breakdown parser this feeds needs a K8s-native replacement outside this file
- [ ] [HIGH] lines 65-69: `docker_login()` — `docker login -u {username} -p {password} {url}`
      → k3s uses containerd; needs `kubectl create secret docker-registry` (imagePullSecret) or `crictl`/`ctr` login — decide whether Docker CLI is retained on the node just for registry auth
- [ ] [HIGH] lines 71-77: `docker_inspect_manifest()` — `docker manifest inspect {image_tag}`
      → replace with a registry API call, or `crane`/`skopeo manifest inspect`, decoupled from local Docker daemon state
- [ ] [HIGH] lines 106-107: `bench_init()` renders `docker-compose.yml.jinja2`
      → K8s manifest generation instead of the Swarm-stack-file pattern
- [ ] [HIGH] lines 109-115: `bench_init()` — `docker run --rm --net none -v {config_directory}:/.../configmount {image} cp -LR config/. configmount` (extract baked-in config files from the image via a throwaway container)
      → needs a Job/init-container pattern, or `crictl`/skopeo image-export, or continued `docker run --rm` if Docker stays available on the node for one-off extraction
- [ ] [HIGH] lines 117-124: `bench_init()` — identical throwaway-container pattern for `sites/` directory extraction
      → same fix as above
- [ ] [HIGH] lines 151-163: `container_exists()` — `docker inspect {name}` retried with backoff, asserts a container does NOT exist before proceeding
      → `kubectl get pod/deployment {name}` existence check; "container" vs. "pod/deployment" semantics need mapping
- [ ] [HIGH] lines 165-173: `get_image_size()` — `docker image ls --format ... | grep -E {pattern}`
      → `crictl images` / registry API / `ctr image ls`
- [ ] [HIGH] lines 175-183: `unused_image_size()` — `docker image ls` + `docker container ls --format {{.Image}}` diffed to find unused images
      → containerd-native equivalents (`crictl images`/`crictl ps`) or a registry-side GC report
- [ ] [MEDIUM] line 186: docstring "Checks archived (bench and site) and unused docker artefacts size"
      → wording update; the underlying calls above carry the actual fix
- [ ] [HIGH] lines 219-225: `_force_remove_zombie_benches()` — `docker ps --all --filter "name=^{bench_name}$" | grep {bench_name}` as an existence probe
      → K8s equivalent existence check (`kubectl get pod -l bench={name}`)
- [ ] [HIGH] lines 245-249: `_push_images_to_registry()` — `docker_login()` then `docker push {image}` per image
      → containerd/registry push tooling (`ctr image push`, `crane push`), or keep `docker push` if build/push tooling stays separate from the runtime (see builder.py)
- [ ] [HIGH] line 286: `disable_production_on_bench()` — `docker rm {name} --force`
      → `kubectl delete deployment/pod {name} --force`
- [ ] [HIGH] line 382: `_stop_bench_workers()` calls `bench.docker_execute(...)`
      → follows the bench.py rename, call site only
- [ ] [HIGH] line 395: `_start_bench_workers()` calls `bench.docker_execute(...)`
      → follows the bench.py rename, call site only
- [ ] [HIGH] line 407: `_force_remove_all_benches()` — `bench.execute(f"docker rm {bench.name} --force")`
      → `kubectl delete` equivalent
- [ ] [HIGH] lines 434-440: `remove_docker_image()` — `docker_inspect_manifest()` then `docker rmi {image_tag} --force`
      → registry-manifest check (above) + `crictl rmi`/`ctr image rm` for local removal; "local image on this host" semantics differ once images are pulled cluster-wide
- [ ] [HIGH] lines 442-446 & 504-513: `cleanup_unused_files()` → `remove_unused_docker_artefacts()` — `docker system df -v` before/after, `docker system prune -af`
      → containerd image/GC equivalents (`crictl rmi --prune`); k3s already auto-GCs images per its own policy, so this step may become unnecessary or need reimplementation as a k3s GC threshold tweak
- [ ] [HIGH] lines 452-456: `remove_benches_without_container()` — `docker ps -a | grep {bench}` as an existence probe
      → `kubectl get pods`
- [ ] [HIGH] lines 736-746: `pull_docker_images()` / `_pull_docker_images()` — `docker_login()` + `docker pull {image_tag}` per image, on this specific host
      → `crictl pull`/`ctr image pull`, or a K8s-native pre-pull DaemonSet/Job, since K8s scheduling is cluster-wide rather than "pull on this specific host"
- [ ] [HIGH] line 874: `get_running_bench_containers()` — `docker ps --format '{{.Names}}'`
      → `kubectl get pods -o name` (or deployments); pod names are often generated/suffixed rather than exactly the bench name, needs care in the cross-check against `Server.benches`
- [ ] [HIGH] lines 1026 & 1040-1042: `get_storage_breakdown()` — `docker system df --format '{{.Size}}'` parsed via `parse_docker_df_output` into `app_storage_analysis["docker"]`
      → containerd storage stats source; rename the resulting key/label

### Stays unchanged:
- `setup_nginx`, `_generate_nginx_config`, `_generate_agent_nginx_config`, `_generate_redis_config`, `_generate_supervisor_config` — config generation, host-filesystem/nginx, not runtime-dependent
- `_update_supervisor`, `update_agent_web`, `update_agent_cli` — supervisor management for the agent process itself
- `_add_to_acl`/`_remove_from_acl` — NFS ACL management
- `stats`, `processes`, `mariadb_processlist`, `_memory_stats`, `_cpu_stats` — host/process/DB reporting
- `update_site_pull_job`, `update_site_migrate_job`, `move_site` — bench/site orchestration jobs operating on Frappe domain logic
- `archive_bench()` — its own body only touches `Bench`/`docker_image` attributes and calls the already-listed HIGH items; no additional change needed beyond those

---

## site.py
1847 lines. No direct `import docker` — all container access goes through
`self.bench.docker_execute(...)` or this file's own thin wrapper `bench_execute()`, plus
plain DB-client calls that run directly against the DB host and never touch the
container runtime.

### Must change:
- [ ] [HIGH] line 62: `bench_execute()` — `return self.bench.docker_execute(f"bench --site {self.name} {command}", input=input)`
      → the single most important call site in this file: once `Bench.docker_execute` becomes `k8s_execute`, this flows through automatically, but ~30+ callers throughout the file (install/uninstall apps, migrate, backup, maintenance mode, scheduler, cache, user creation, etc.) are all transitively dependent on it
- [ ] [HIGH] line 515: `keys = self.bench.docker_execute(keys_command)` in `reset_site_usage()`
      → call site only
- [ ] [HIGH] lines 518-519: `self.bench.docker_execute(f"redis-cli -p 13000 ...")` (GET/DEL) in `reset_site_usage()`
      → call sites only
- [ ] [MEDIUM] lines 533-543: `build_bypass_unlink_shim` docstring reasoning — "matches the container without arch juggling — Docker on Linux shares the host architecture"
      → the LD_PRELOAD shim-build strategy assumes the backup command runs on the same host/architecture as the container via `docker exec` sharing the kernel; under k3s a pod can be scheduled on any node, potentially a different architecture/filesystem namespace — needs redesign (per-node build, per-arch prebuilt artifact, or an init-container build)
- [ ] [MEDIUM] lines 580-582: `backup()` comment reiterating the same host-architecture-sharing assumption as above
      → same underlying issue as the shim-build item; the mechanism it justifies is HIGH (below), the comment itself is MEDIUM
- [ ] [HIGH] lines 584-591: `backup()` — computes `docker_backup_dir`/`host_backup_dir`/`docker_so_path`/`host_so_path`, translating an in-container path to a host-mounted path assuming a direct bind-mount between the Docker host and container filesystem
      → this bind-mount relationship may not hold under K8s depending on the chosen volume/PVC backend — needs re-verification/redesign per the storage plan
- [ ] [HIGH] line 624: `self.bench.docker_execute(backup_command)` in `backup()`
      → call site only
- [ ] [HIGH] lines 1154-1155 & 1160: `terminate_in_container_backup()` — comment explains processes exec'd via `docker exec` do NOT die when the host-side exec client dies, so `self.bench.docker_execute(f"pkill -f {todays_dt}", ...)` kills them explicitly
      → architectural gap #6: `kubectl exec`'s orphaning semantics need verification under containerd/k3s before trusting the same `pkill`-based cleanup; the call site itself follows the docker_execute rename
- [ ] [HIGH] lines 1170 & 1173: comment "docker_execute chdirs into the dir (-w)" then `self.bench.docker_execute(f"mkdir -p {relative_path_to_backup_directory}")`
      → call site only, but the comment should describe `kubectl exec`'s equivalent (no native `-w`; needs `sh -c "cd X && ..."` wrapping)
- [ ] [HIGH] line 1175: `self.bench.docker_execute(f"mkfifo {file}", subdir=...)`
      → call site only
- [ ] [LOW] line 1348: comment referencing "the docker exec client"
      → prose update for accuracy once the exec mechanism changes

### Stays unchanged:
- Site config CRUD, `install_apps`/`uninstall_app`
- `restore_site`/`restore_files` (tar extraction logic via `_safe_extract_tar`)
- Backup file management: `tablewise_backup`, `restore_touched_tables`, `drop_new_tables`, `fetch_latest_backup`, `calculate_checksum_of_backup_files`
- S3/offsite upload (`upload_offsite_backup` and related streaming logic)
- Database user/permission management: `create_database_access_credentials`, `db_instance`, `run_sql_query`
- Scheduler/maintenance-mode toggles and the `@job`/`@step` wrappers that merely call `bench_execute()`
- Direct DB access via `db_client_cli()`/`self.execute()` (`tables`, `timezone`, `get_database_size`, `describe_database_table` fallback query) — talks straight to MariaDB over the network, entirely runtime-agnostic

---

## proxy.py

### Verification Note (follow-up check, re-read in full a second time)
Sanity check requested because this project's architecture already decided to use
Traefik (k3s's built-in Ingress Controller) for routing, and a "0 changes" verdict on
the proxy file needed confirming as *actually* zero, not skipped.

**1. Does this file generate/manage nginx config, or only call a proxy assumed to
already exist?** It generates and manages real nginx config directly.
`_generate_proxy_config()` (line 341) renders `proxy/nginx.conf.jinja2` into a live
`proxy.conf` file from `self.hosts`/`self.upstreams`/`self.wildcards`;
`add_host`/`add_wildcard_hosts`/`add_upstream`/`add_site_to_upstream`/`setup_redirect`/
`update_site_status` all write real files under `hosts_directory`/`upstreams_directory`
(`map.json`, `redirect.json`, per-site status files, TLS cert symlinks) that the
rendered config reads; `setup_proxy()`/`_reload_nginx()` actually trigger
`sudo systemctl reload nginx` / `NginxReloadManager().request_reload(...)`. This is a
complete, active config-generation-and-reload component, not a stub.

**2. Any reverse-proxy logic assuming Docker networking (container IPs, `docker
network inspect`, etc.)?** No. Re-confirmed via a full second read (498/498 lines) and
`grep -n -i docker` returning zero matches. No container-IP resolution, no Docker
API/CLI call of any kind anywhere in the file. The upstream *target* values it writes
to disk are opaque strings handed to it by callers (bench.py's port-mapping logic
decides what those strings actually are) — proxy.py never computes or interprets a
Docker network address itself.

**3. Is the nginx→Traefik transition already handled entirely outside this file, or
does this file need updates we missed?** Partially — and the two things aren't quite
the same migration, worth being precise about:
- At the **code level** (does anything in proxy.py need to change because it calls
  Docker): nothing to change — confirmed above, 0 Docker references. The item count
  for this file stays 0/0/0, unchanged from the first pass.
- At the **architecture level** (does proxy.py's *function* still need to exist once
  Kagent is fully K8s-native): less settled than "already handled entirely outside
  this file." proxy.py's nginx tier and this project's Traefik/IngressRoute work solve
  *related but not identical* problems. proxy.py runs on Frappe Cloud's own dedicated
  Proxy Servers, doing Host-header-based routing across *many separate bench servers*,
  plus redirects, wildcard domains, per-site maintenance/suspended-status toggling, and
  **weighted auto-scale routing across secondary upstreams**
  (`secondaries.json`/`set_secondaries_for_upstream`, lines 386-421). This project's
  Traefik testing so far has proven the routing and maintenance-mode pieces at the
  *single-bench-pod* level (D11: a bench needs a Service before an IngressRoute means
  anything; K5: a real IngressRoute wired to a real Service; M1: adding a domain via a
  new IngressRoute; Tier C7/C9: Host-based routing plus a `router.middlewares`
  annotation referencing a maintenance Middleware) — these line up well with
  `add_host`/`update_site_status`. **Not yet proven anywhere in this project's testing:
  a Traefik-native equivalent of the weighted secondary/auto-scale routing.** Tier C9's
  own finding also notes the maintenance Middleware referenced by the tested annotation
  didn't actually exist yet at that point ("this annotation references a middleware
  that doesn't exist ... would have no actual effect until created") — so even the
  closest-matching piece was validated at the annotation-mechanism level, not as a
  complete working example.

**Bottom line:** proxy.py needs no Docker-runtime code changes — the original verdict
for the file itself was correct, not skipped. But whether the whole file becomes dead
code under a fully K8s-native Kagent, or whether some of its logic (specifically the
weighted-secondaries/auto-scale piece) still needs to exist until a proven Traefik
equivalent is built and tested, is an open architecture question this file-level scan
can't resolve on its own — flagging for a decision before treating the file as simply
"done."

---

498 lines. **Zero Docker references of any kind** — confirmed via `grep -n -i docker`
returning no matches.

### Must change:
None. This file is entirely nginx/host-filesystem based (upstream directories, host
directories, `map.json`/`redirect.json` files, `filelock`-guarded config mutation,
`sudo systemctl reload nginx`). It has no direct dependency on the container runtime.

### Stays unchanged:
- The whole file — `Proxy` (extends `Server`) manages nginx-based routing (hosts,
  upstreams, wildcards, secondaries/auto-scale weighting, redirects) purely through
  local files and `systemctl reload nginx`/`NginxReloadManager`. This sits above the
  container-runtime abstraction entirely.
- **Indirect dependency to verify, not a code change here:** if the K8s migration
  changes how bench/site containers are addressed (pod IPs vs. fixed
  `127.0.0.1`-bound ports), the *upstream target* values fed into this module from
  `bench.py`'s port-mapping logic will change, which could ripple into
  `add_site_to_upstream`/`_generate_proxy_config`'s target resolution. Flag as a
  downstream check during implementation, not a required change in this file.

---

## database_server.py
1281 lines. **Zero Docker references of any kind** — confirmed via `grep -n -i docker`
returning no matches.

### Must change:
None. This file manages MariaDB directly (replication, binlog purge/indexing, audit log
rotation/upload, table check/repair, schema-size metadata) via `MySQLDatabase`/`peewee`/
`Database` client connections and plain host filesystem paths (`/var/lib/mysql`,
`/var/lib/pt-stalk`, `/var/log/mysql`). MariaDB runs as a systemd/bare-metal service on
the database server here, not inside a Docker container this agent manages.

### Stays unchanged:
- The entire file — every operation is either (a) MariaDB protocol calls via
  `MySQLDatabase`/`Database`/`CustomPeeweeDB`, (b) local filesystem operations on
  binlog/audit-log/pt-stalk directories, or (c) S3 upload via `boto3`. None of it
  depends on whether the bench/site application layer runs in Docker or K8s.
- **Confirm with stakeholders:** this analysis assumes DatabaseServer nodes stay
  non-containerized in the target Kagent architecture (consistent with `frappe/agent`'s
  existing design) — this file gives no evidence either way on its own, since it simply
  never touches Docker regardless of that decision.

---

## builder.py
942 lines. **`import docker` at line 18** — the only one of the six files with a direct
Python Docker SDK dependency, via `docker.from_env(...)` (lines 671, 902) and
`client.images.push(...)` (lines 709, 908). This makes it the highest-friction file to
migrate: it can't be a simple call-site rename, the SDK client object and its push API
need a real replacement.

### Must change:
- [ ] [HIGH] line 18: `import docker`
      → remove/replace per the builds-architecture decision (gap #4); if a Docker daemon is retained on dedicated build hosts this may stay, otherwise needs a containerd/buildkit-native client (`python-on-whales`, or shelling to `nerdctl`/`buildctl`/`crane`)
- [ ] [HIGH] line 625: `command = f"docker buildx build --platform {self.platform}"` in `_get_build_command()`
      → architectural gap #4: the core image-build mechanism (BuildKit via Buildx, tarred context piped via stdin) — decide kaniko/in-cluster build vs. retained external Docker/BuildKit hosts
- [ ] [HIGH] lines 646-654: `_get_build_environment()` — sets `DOCKER_BUILDKIT=1`, `BUILDKIT_PROGRESS=plain`, `PROGRESS_NO_TRUNC=1`
      → changes in lockstep with the line 625 decision
- [ ] [HIGH] lines 668-671: `_push_docker_image()` — `client = docker.from_env(environment=environment, timeout=5*60)`
      → Docker SDK client instantiation; replace per the SDK-removal decision (gap #5)
- [ ] [HIGH] lines 703-716: `_push_image()` — `client.images.push(self.image_repository, self.image_tag, stream=True, decode=True, auth_config=auth_config)`
      → architectural gap #5: replace with registry-native push (`crane push`, `skopeo copy`, `nerdctl push`) decoupled from a local Docker daemon
- [ ] [HIGH] lines 813-823: `_start_base_container()` (used by `PatchImageBuilder`) — `docker login`, `docker pull {base_image}`, `docker run -d --name {container_name} {base_image} tail -f /dev/null` to start a long-lived container to patch in place
      → same "commit a live container" gap as bench.py's `commit_container_changes`; needs a real rebuild-based patch flow or a K8s Job/Pod performing the same steps followed by a proper build+push
- [ ] [HIGH] lines 837-874: `_docker_exec()` and its callers (`_pull_app`, `_has_ui_changes`, `_has_dependency_changes`, `_reinstall_app_deps`, `_bench_build_app`) — `self.execute(f"docker exec {self.container_name} bash -c {shlex.quote(command)}")`
      → same class of fix as `Bench.docker_execute`, scoped to `PatchImageBuilder`'s standalone container; better redesigned entirely around K8s Jobs/kaniko rather than "run container, exec into it, commit it"
- [ ] [HIGH] lines 893-897: `_commit_patch_image()` — `docker commit --change='CMD ["supervisord"]' {container_name} {image_name}`
      → architectural gap #1: same "commit a live container" gap, no K8s equivalent; needs a Dockerfile-based incremental build or an image-layer diff/export tool compatible with the K8s-based build pipeline
- [ ] [HIGH] lines 899-919: `_push_patch_image()` — identical Docker SDK push pattern as `_push_docker_image`/`_push_image`
      → same fix as those two items
- [ ] [HIGH] lines 921-923: `_cleanup_container()` — `self.execute(f"docker rm -f {self.container_name}")`
      → `kubectl delete job/pod` if patch-build is redesigned around K8s Jobs, or stays as-is if Docker is retained for builds specifically
- [ ] [LOW] line 934: `get_builds_directory()` returns a path literally named `.docker-builds`
      → cosmetic rename once the build backend is finalized, low priority
- [ ] [LOW] lines 116, 275, 288, 293, 316-317, 528, 551: various `dockerfile`/`Dockerfile` references (parameter names, template variable, the literal `Dockerfile` written to the build context, `_inject_additional_packages` patching `Dockerfile` content)
      → these are about the Dockerfile *build format*, not the Docker runtime; very likely retained regardless of build tool (BuildKit/kaniko/buildx all consume standard Dockerfiles) — informational only, no required change unless the build tool changes to something that doesn't consume Dockerfiles

### Stays unchanged:
- `ContextManager`/`ValidationManager` classes — git cloning (`_run_git_command`, `_clone_repository`), build-context assembly (`_copy_build_config_files`, `_generate_build_config_files`, `prepare_build_context`), dependency/package-manager validation (`check_python_syntax`, `get_package_manager_files`, Python/Node version checks, Frappe app-dependency checks) — pure filesystem/git/parsing logic, no container-runtime dependency
- `tar_build_context()`/`_cleanup_context()` — tars the build directory into a context archive; runtime-agnostic since BuildKit, kaniko, and Docker buildx all accept a tar'd context
- `_run()` (lines 734-763) — the generic subprocess runner (`Popen` + `shlex.split`) streaming a build command's stdout; reusable regardless of which command string it's given, only the command string built in `_get_build_command()` (HIGH item above) needs to change

---

## Execution Order (priority queue)

All HIGH items across all files, in a sensible dependency order — foundational
call-through mechanisms first (renaming `docker_execute`/`k8s_execute` unlocks every
call site that depends on it), then the standalone lifecycle/build mechanisms, then
architectural-gap items last since those need a design decision before any code lands:

1. bench.py line 177: `docker_execute()` definition → `k8s_execute()`
2. bench.py line 61: `docker_image` config field → K8s image reference
3. bench.py line 206: `docker_execute()` call in `bench_new_site`
4. bench.py line 449: `docker_execute()` call in `bench_archive_site`
5. bench.py line 582: `docker_execute()` call in `restart()`
6. bench.py lines 591, 594, 596: `docker_execute()` calls in `rebuild()`
7. bench.py lines 699, 700: `docker_execute()` calls in `update_supervisor()`
8. bench.py lines 762, 764-766, 767: `docker_execute()` calls in `_start_code_server()`
9. bench.py line 771: `docker_execute()` call in `_stop_code_server()`
10. bench.py line 792: `docker_execute()` call in `remove_code_server()`
11. bench.py line 1056: `docker_execute()` call in `git_apply()`
12. bench.py line 1067: `docker_execute()` call in `run_supervisorctl_command()`
13. bench.py line 1126: `exec` partial definition in `_pull_app_change()`
14. bench.py lines 1132, 1136, 1140, 1141, 1144, 1145, 1146, 1149: git calls via that partial
15. bench.py lines 1159, 1169, 1174: `docker_execute()` calls in `set_git_remote()`
16. bench.py line 1189: `docker_execute()` call in `setup_requirements()`
17. bench.py line 1399: `docker_execute()` call in `_inactive_scheduler_sites()`
18. bench.py line 347: `docker top` in `get_worker_pids()`
19. server.py lines 382, 395: `bench.docker_execute()` calls in `_stop_bench_workers`/`_start_bench_workers`
20. site.py line 62: `bench_execute()` → the ~30+-caller fan-out point
21. site.py line 515, 518-519: `docker_execute()` calls in `reset_site_usage()`
22. site.py line 624: `docker_execute()` call in `backup()`
23. site.py lines 1154-1155, 1160: `docker_execute()` call in `terminate_in_container_backup()`
24. site.py lines 1170, 1173: `docker_execute()` call (mkdir) in the backup path
25. site.py line 1175: `docker_execute()` call (mkfifo) in the backup path
26. site.py lines 584-591: backup path host/container path translation
27. server.py lines 65-69: `docker_login()`
28. server.py lines 71-77: `docker_inspect_manifest()`
29. server.py lines 151-163: `container_exists()`
30. server.py lines 165-173: `get_image_size()`
31. server.py lines 175-183: `unused_image_size()`
32. server.py lines 219-225: `_force_remove_zombie_benches()`
33. server.py lines 245-249: `_push_images_to_registry()`
34. server.py line 286: `disable_production_on_bench()`
35. server.py line 407: `_force_remove_all_benches()`
36. server.py lines 434-440: `remove_docker_image()`
37. server.py lines 442-446, 504-513: `remove_unused_docker_artefacts()`
38. server.py lines 452-456: `remove_benches_without_container()`
39. server.py lines 736-746: `pull_docker_images()`/`_pull_docker_images()`
40. server.py line 874: `get_running_bench_containers()`
41. server.py lines 1026, 1040-1042: `get_storage_breakdown()`
42. server.py lines 106-107, 109-115, 117-124: `bench_init()`'s compose-file render + extraction containers
43. builder.py line 625: `_get_build_command()` — `docker buildx build`
44. builder.py lines 646-654: `_get_build_environment()`
45. builder.py line 18: `import docker`
46. builder.py lines 668-671: `_push_docker_image()`
47. builder.py lines 703-716: `_push_image()`
48. builder.py lines 899-919: `_push_patch_image()`
49. builder.py lines 813-823: `_start_base_container()`
50. builder.py lines 837-874: `_docker_exec()` and its callers
51. builder.py lines 921-923: `_cleanup_container()`
52. **[architectural gap #2]** bench.py lines 822-868: `start()` — Deployment/Service/PVC redesign
53. **[architectural gap #2]** bench.py lines 870-874, 878, 882: `stop()`/`_stop()`/`_start()`
54. **[architectural gap #3]** bench.py lines 899-915: `_update_runtime_limits()` — `kubectl patch` resources
55. **[architectural gap #1]** bench.py lines 1234-1239: `commit_container_changes()` — needs a rebuild-based redesign
56. **[architectural gap #1]** builder.py lines 893-897: `_commit_patch_image()` — needs a rebuild-based redesign
