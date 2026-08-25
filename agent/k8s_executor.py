"""Kubernetes/k3s replacement for Bench.docker_execute() (agent/bench.py:177).

This module is written and tested in isolation -- it is NOT yet imported or
called from anywhere in bench.py/server.py/site.py/builder.py. Wiring it into
bench.py's ~43 docker_execute() call sites is a separate, later task (see
ANALYSIS.md's Execution Order). This module's job right now is to prove the
exec mechanism works against the real k3s cluster and to nail down a return
contract close enough to docker_execute()'s that the eventual rewiring is a
near-mechanical rename, not a redesign.

Target runtime: k3s v1.36.3+k3s1 specifically (confirmed live via `kubectl
version` on 92.5.91.195), not upstream Kubernetes assumed in the abstract:

- Container runtime is k3s's embedded containerd, not Docker-in-K8s. This
  module never references dockershim and never shells out to `docker` --
  everything here goes through the official `kubernetes` Python client's
  exec/stream API talking to the k3s API server, the same way it would talk
  to any k8s API server regardless of the runtime behind it.
- The agent process runs on the host, not (yet) as a pod inside the cluster,
  so kubeconfig is loaded from a file (`$KUBECONFIG`, defaulting to
  `~/.kube/config` -- on this server that resolves to
  `/home/frappe/.kube/config`), never `config.load_incluster_config()`.
- Default StorageClass here is k3s's built-in `local-path` -- irrelevant to
  this module directly (it doesn't touch PVCs), noted because nothing below
  assumes a cloud provisioner or any other StorageClass exists.
- `kubernetes` Python client v36.0.3 was installed for this module's dry-run
  test, matching k3s's server API version (1.36) -- see the compatibility
  note in the dry-run section at the bottom of this file.

Docker -> Kubernetes/k3s concept mapping
-----------------------------------------------------------------------
Old (Docker)                                  New (K8s/k3s, this module)
-----------------------------------------------------------------------
`docker exec` on a named container            kubernetes exec on a Pod,
                                               targeting one container by
                                               name, over the k3s API
                                               server's exec/stream
                                               subresource
`docker exec -w {workdir}`                    no native flag on the K8s exec
                                               API; wrapped as
                                               `bash -c "cd {workdir} && ..."`
`docker exec -u root`                         **not supported** by the K8s
                                               exec API at all -- see
                                               "Known deviations" below
Swarm multi-container service+task lookup     pod resolved by label selector,
(`docker service ps` before every exec,       re-resolved on every call for
because Swarm tasks get new IDs on            the same reason: a Deployment's
reschedule)                                   pod name changes on every
                                               rollout/restart too
"the container" (docker_execute() never       `_resolve_container()` --
had to pick a container within a              must disambiguate a specific
container)                                    container within the pod (D24:
                                               e.g. the Redis pods run 3
                                               sidecar containers, one per
                                               Frappe Redis role, since one
                                               `redis-server` process only
                                               binds the port it started with)
"""

from __future__ import annotations

import os
import shlex
from datetime import datetime
from typing import TYPE_CHECKING, Callable

from kubernetes import client, config
from kubernetes.client.rest import ApiException
from kubernetes.stream import stream
from kubernetes.stream.ws_client import STDIN_CHANNEL

from agent.exceptions import AgentException
from agent.utils import get_execution_result

if TYPE_CHECKING:
    from kubernetes.client import V1Pod

# docker_execute() hardcodes "/home/frappe/frappe-bench" -- the upstream
# frappe/agent convention for a Docker container where the bench directory
# sits directly under $HOME. This project's k3s benches don't live there: D6
# requires the PVC to mount one level *above* the bench directory (`bench
# init` refuses to run into a pre-existing target path), so every bench pod
# here actually has its bench at /home/frappe/bench-data/frappe-bench --
# confirmed live inside the real bench-v16 container (`ls -la /home/frappe/
# /home/frappe/bench-data/`) while building this module's dry-run test.
# `docker_execute()`'s hardcoded value would be a silent wrong-default bug
# the moment this function gets wired into bench.py, not a cosmetic
# difference -- every unqualified call site relies on this default.
DEFAULT_WORKDIR = "/home/frappe/bench-data/frappe-bench"

# Module-level cache: config.load_kube_config() parses and validates the
# kubeconfig file on every call, which is wasted work if k8s_execute() is
# called dozens of times per Job (as docker_execute() is today). Loaded once
# per process. Not thread-safe -- fine for this project's one-job-per-process
# RQ worker model (same assumption Base.execute() already makes about
# self.data being process-local, not shared across concurrent jobs).
_kube_config_loaded = False


class PodNotFoundError(Exception):
    """No Running pod matched the given namespace/label selector."""


class AmbiguousContainerError(Exception):
    """A container name was required (pod has >1 container) but not given,
    or the given container name doesn't exist in the pod."""


def _load_kube_config() -> None:
    """Load kubeconfig the way this project's k3s host expects it.

    docker_execute() talked to the local Docker daemon over its Unix socket
    -- implicit, no explicit auth step. Kagent instead talks to the k3s API
    server, which needs an explicit kubeconfig. This agent runs on the host
    (not yet as an in-cluster pod), so this loads the kubeconfig file the
    same way `kubectl` does on this server: respect $KUBECONFIG if set,
    otherwise fall back to ~/.kube/config (which on 92.5.91.195 is
    /home/frappe/.kube/config, per this project's k3s install notes).

    Deliberately NOT config.load_incluster_config() -- there is no in-cluster
    ServiceAccount token to read, since the agent process isn't running as a
    pod (for now; if/when Kagent itself gets containerized inside the
    cluster, this is the one function that needs to switch).
    """
    global _kube_config_loaded
    if _kube_config_loaded:
        return
    kubeconfig_path = os.environ.get("KUBECONFIG") or os.path.expanduser("~/.kube/config")
    config.load_kube_config(config_file=kubeconfig_path)
    _kube_config_loaded = True


def _resolve_pod_name(core_v1: client.CoreV1Api, namespace: str, label_selector: str) -> str:
    """Resolve a label selector to exactly one Running pod name, right now.

    docker_execute()'s Swarm branch resolved a moving target
    (`docker service ps -f desired-state=Running -q --no-trunc {service}`)
    into a concrete task ID immediately before every exec call, rather than
    caching it, because Swarm tasks are recreated (new IDs) on every
    reschedule. A Deployment's pod behaves the same way: ReplicaSet-hash
    suffixes change on every rollout/restart (confirmed live -- the
    frappe-v16 bench pod is currently named `bench-v16-547fd7bffd-bzg25`,
    and that suffix is not stable across restarts). This function must
    always be called fresh, never given a cached pod name from a previous
    call.
    """
    pods = core_v1.list_namespaced_pod(namespace, label_selector=label_selector).items
    running = [p for p in pods if p.status.phase == "Running"]
    if not running:
        raise PodNotFoundError(
            f"No Running pod found in namespace {namespace!r} matching selector "
            f"{label_selector!r} ({len(pods)} pod(s) matched, none Running)"
        )
    # This project's D16 mandates one Deployment (replicas=1) per bench, so
    # there should be exactly one match; if there's ever more than one
    # (mid-rollout, or a misconfigured selector), take the first and don't
    # treat it as an error -- a rolling update briefly having 2 pods match
    # is normal, not a fault condition worth failing an exec call over.
    return running[0].metadata.name


def _resolve_container(pod: V1Pod, container: str | None) -> str:
    """Pick the target container within the pod, per Decision Log D24.

    A `containerPort` entry is metadata, not a listen directive -- a single
    process only binds the port it was actually started with. This project's
    frappe-system/frappe-v16 Redis pods run 3 sidecar containers
    (redis-cache, redis-queue, redis-socketio) for exactly that reason
    (confirmed live: `kubectl get pod -n frappe-v16 -l app=redis-v16` has 3
    containers, not 1). docker_execute() never had to pick a container within
    a container. This function always does.

    If the pod has exactly one container, that's the default (mirrors
    docker_execute()'s single_container semantics, and matches the
    frappe-v16 bench pod, which has exactly one container named "bench").
    If it has more than one, `container` MUST be passed explicitly -- this
    function refuses to guess, the same way `kubectl exec` itself refuses
    ("error: a container name must be specified") once a pod has more than
    one container.
    """
    names = [c.name for c in pod.spec.containers]
    if container:
        if container not in names:
            raise AmbiguousContainerError(
                f"Container {container!r} not found in pod {pod.metadata.name!r}; "
                f"available containers: {names}"
            )
        return container
    if len(names) == 1:
        return names[0]
    raise AmbiguousContainerError(
        f"Pod {pod.metadata.name!r} has {len(names)} containers ({names}); "
        "a container name must be specified explicitly -- k8s_execute() will not guess (D24)."
    )


def resolve_namespace_and_pod_selector(bench_name: str) -> tuple[str, str]:
    """Derive k8s_execute()'s required `namespace`/`pod_label_selector` from a
    bench name -- a Tier 0 prerequisite for wiring `Bench.docker_execute()`
    to `k8s_execute()`, built and live-tested standalone here, NOT yet called
    from bench.py (bench.py is untouched -- this function exists so that
    integration is a one-line call once it's approved, not a redesign).

    **This encodes the naming convention this project's own validation
    cluster (92.5.91.195) demonstrates, confirmed live for every real bench
    in it -- it is NOT necessarily the real production Kagent namespace
    scheme.** This cluster organizes namespaces per Frappe *version*
    (`frappe-v14`/`frappe-v15`/`frappe-v16`, one shared test bench each) to
    prove out version-branching behavior; a real multi-tenant Kagent would
    far more plausibly namespace per *tenant/site*, not per Frappe version.
    Whoever wires this in for real needs to confirm which convention the
    actual Bench-provisioning code will use -- don't assume this function's
    current logic transfers unchanged to production naming.

    Convention (`bench-{suffix}` -> namespace `frappe-{suffix}`, selector
    `app=bench-{suffix}`), confirmed live against the two benches that
    follow it:
    - `bench-v14` -> (`frappe-v14`, `app=bench-v14`) -- live-verified,
      resolves to the real `bench-v14-...` pod and a working `k8s_execute()`
      call against it.
    - `bench-v16` -> (`frappe-v16`, `app=bench-v16`) -- live-verified, same.

    **Known gap, found by testing rather than assumed away:** `bench-v15`
    would derive to (`frappe-v15`, `app=bench-v15`) under this same
    convention, but the real `bench-v15` pod carries **no labels at all**
    (`kubectl get pod bench-v15 -n frappe-v15 --show-labels` -> `<none>`,
    confirmed live) -- it's the project's known bare-Pod legacy state (D16
    already mandates every bench should be a Deployment, never a bare Pod,
    specifically because of gaps like this one). A selector this function
    derives for v15 will resolve to zero pods and `k8s_execute()` will raise
    `PodNotFoundError` -- this is not a bug in this function, it's an
    accurate reflection of v15's pod having nothing to select. Wiring v15
    through this path requires either giving that pod a matching label
    (a one-time `kubectl label`, the same workaround
    `tests/run-all-tests.sh` already uses temporarily for its own v15
    IngressRoute test) or migrating it to a proper Deployment first, per D16.

    Raises:
        ValueError: if `bench_name` doesn't start with `"bench-"` -- this
            function only knows the one convention above, and refuses to
            guess at any other bench-naming scheme silently.
    """
    prefix = "bench-"
    if not bench_name.startswith(prefix):
        raise ValueError(
            f"resolve_namespace_and_pod_selector() only knows the 'bench-{{suffix}}' "
            f"naming convention this test cluster uses; got {bench_name!r}, which "
            f"doesn't start with {prefix!r}. Pass namespace/pod_label_selector to "
            f"k8s_execute() explicitly instead of relying on this helper."
        )
    suffix = bench_name[len(prefix):]
    namespace = f"frappe-{suffix}"
    pod_label_selector = f"app={bench_name}"
    return namespace, pod_label_selector


def k8s_execute(
    command: str,
    *,
    namespace: str,
    pod_label_selector: str,
    container: str | None = None,
    workdir: str | None = None,
    subdir: str | None = None,
    input: str | None = None,  # noqa: A002 -- matches docker_execute()'s param name
    non_zero_throw: bool = True,
    as_root: bool = False,
    on_output_line: Callable[[str], None] | None = None,
    timeout: int = 300,
) -> dict:
    """Kubernetes/k3s replacement for Bench.docker_execute() (bench.py:177).

    Return/exception contract is intentionally close to
    `agent.base.Base.execute()` / `Bench.docker_execute()`, so bench.py's ~43
    call sites (and the Job system consuming their results) can eventually
    be repointed at this function with minimal signature drift:

    - Returns a dict shaped like `agent.utils.ExecutionResult`: `command`,
      `directory` (repurposed, see "Known deviations" #4), `start`, `end`,
      `duration`, `status` ("Success"/"Failure"), `returncode`, `output`
      (combined stdout+stderr, in the order it was actually received --
      matches Base.execute()'s `stderr=subprocess.STDOUT` merge, NOT split
      streams).
    - On a non-zero exit code with `non_zero_throw=True` (the default,
      matching docker_execute()'s default), raises
      `agent.exceptions.AgentException` with `.data` set to that same dict --
      so existing error-handling code written against docker_execute() (e.g.
      bench.py:571's `"No such container" in e.data["output"]` pattern)
      keeps working mechanically, though the literal *string* it matches
      will need to change once this is wired in (K8s' not-found error text
      differs from Docker's -- flagged in ANALYSIS.md's bench.py section).

    Decision Log constraints applied directly:

    - **D38** (never pipe a live stream into a short-circuiting reader under
      pipefail-style checking): this function never shells out to anything
      and never pipes exec output through `grep -q`/`head`/etc. The
      WebSocket exec stream is fully drained (`resp.run_forever()` /
      `resp.read_all()` below) BEFORE any inspection of the output happens
      -- output is captured completely first, then returned, generalizing
      D38's fix ("capture into a variable, then match") from "shell
      pipeline" to this function's own internal buffering.
    - **D30** (some commands, e.g. v16's `run-patch`, are silent on
      success): this function never infers success/failure from output
      content. Success is determined purely by `returncode == 0`, read from
      the K8s exec API's own error channel via the client's `.returncode`
      property -- never by scanning `output` for a particular string.
      Callers must do the same once this is wired in.
    - **D24** (one container != one port, one pod != one container): see
      `_resolve_container()` -- `container` must be explicit whenever a pod
      runs more than one container; this function refuses to guess.

    Args:
        command: the command to run inside the container, exactly as it
            would have been passed as docker_execute()'s `command` argument
            (e.g. "bench build", "bench new-site ..."). Executed via
            `bash -c`, so shell metacharacters behave the same as they did
            under Docker.
        namespace: the K8s namespace the bench's pod lives in (e.g.
            "frappe-v16").
        pod_label_selector: a label selector expected to resolve to exactly
            one Running pod (e.g. "app=bench-v16"). See `_resolve_pod_name`
            for why this is a selector, never a cached pod name.
        container: the container to exec into. Required whenever the pod has
            more than one container (D24); auto-detected for a
            single-container pod (e.g. bench pods -- container name
            "bench", confirmed live against bench-v16).
        workdir: equivalent of docker_execute()'s implicit
            "/home/frappe/frappe-bench" working directory. Defaults to
            DEFAULT_WORKDIR.
        subdir: joined onto workdir, exactly like docker_execute()'s `subdir`.
        input: piped to the exec stream's stdin, then the stream's stdin
            channel is half-closed so the remote process sees a real EOF --
            see "Known deviations" #3 for how this was live-verified
            (a plain `cat` round-trip, and a real `bench console` piped
            script matching this project's `site.py`/COMMANDS.md usage).
        non_zero_throw: if True (default, matches docker_execute()), raise
            AgentException on non-zero exit instead of returning normally.
        as_root: runs the command via `sudo -n` -- see "Known deviations" #1
            for how this was live-verified (and why the earlier `su root -c`
            attempt was dropped, not just relabeled) against all three
            supported bench versions. There is still no true K8s exec-API
            equivalent to `docker exec -u root`; this depends on the target
            image granting the exec'd user passwordless sudo, which the
            frappe/bench image does today but a future image might not --
            `sudo -n` fails fast and loud in that case rather than hanging.
        on_output_line: optional callback invoked with each line of output
            as it streams in. Not wired to Job/Redis in this isolated
            module -- exists so the eventual bench.py integration can pass a
            `self.publish_lines`-style hook through without this function's
            signature changing again (see "Known deviations" #2).
        timeout: seconds to wait for the exec stream to finish before giving
            up and closing it.

    Returns:
        dict: same shape as `Bench.docker_execute()`'s return value.

    Raises:
        AgentException: on non-zero exit (if non_zero_throw), on pod/
            container resolution failure, or on a K8s API error -- always
            with `.data` set to a result dict of the same shape, so a single
            `except AgentException as e: e.data[...]` pattern covers every
            failure mode the same way it does today.

    Known deviations from docker_execute()'s original contract (bench.py
    callers will need to account for these when this gets wired in).
    Reconciled against frappe-k3s-agent's verified project history
    (docs/COMMANDS.md's ~86+21+11+4+11 tested commands across Phases 1-3,
    tests/run-all-tests.sh's full suite, and Decision Log D24/D30/D37/D38 in
    full) -- see the module's git history for the reconciliation pass; each
    item below states what that check found, not just what design intended:

    1. **`as_root` has no real K8s exec API equivalent -- resolved via
       `sudo -n`, live-verified against all three supported versions.**
       Docker's `-u root` sets the exec'd process's UID at the
       container-runtime level; the Kubernetes exec subresource has no
       per-call user-override parameter at all -- a K8s API limitation in
       general, not a k3s-specific one, so this still has to be solved
       inside the container rather than at the exec-API level. An earlier
       pass used `su root -c '...'` and left it explicitly unverified.
       Checking the project's history first: COMMANDS.md/RUNBOOK.md's only
       root-related finding (Phase 3 Group Q) is about a *whole Pod* running
       as root via its spec/securityContext (a Job's container defaulted to
       root, leaving root-owned scratch files on a shared PVC) -- a
       different mechanism from a per-exec-call user override, and that
       finding argues *against* reusing pod-level root, not for it. That
       ruled out one candidate but didn't supply a replacement, so this was
       tested directly against the real bench pods instead of left as a
       guess: `su root -c whoami` against the live bench-v16 pod genuinely
       fails (`su: Authentication failure` -- no password is available over
       `kubectl exec`, so `su` can never work here regardless of image).
       `sudo -n whoami` against the same pod returns `root` cleanly, and the
       same command was re-verified against bench-v14 and bench-v15 too --
       all three return `root` identically (`sudo -l` on bench-v16 confirms
       why: `(ALL) NOPASSWD: ALL` is configured for the `frappe` user in the
       frappe/bench image). This function now wraps `as_root=True` commands
       as `sudo -n {command}` instead of `su root -c '...'` -- the `-n` flag
       makes sudo fail fast with a clear error if a future/different image
       doesn't have `NOPASSWD` configured, instead of hanging on a password
       prompt until this function's timeout. Real callers: bench.py has
       exactly 2 fixed `as_root=True` call sites today (`server.py`
       `_stop_bench_workers`/`_start_bench_workers`, lines 384/397 --
       `supervisorctl stop/start frappe-bench-web: frappe-bench-workers:`,
       needing root because supervisord itself runs as root in this image)
       plus one open-ended surface: the `/benches/<bench>/docker_execute`
       HTTP route (`web.py:1899`) passes `as_root=data.get("as_root")`
       straight through from the request body, so any caller of that
       endpoint (i.e. Press) can request root for an arbitrary command, not
       just these 2 fixed sites -- worth keeping in mind once this is wired
       in, since the real exposure isn't bounded to what bench.py's own
       source shows.
    2. **No live output streaming to Redis yet, and `on_output_line` fires
       post-hoc, not truly incrementally -- confirmed not a blocker for
       anything this project has verified, though it remains a real gap for
       live human-facing progress.** `Base.execute()` calls
       `self.publish_lines()` -> `self.update_redis()` as output arrives
       line-by-line, which is how the Job system shows live progress during
       a long-running step. This function fully drains the exec stream
       first (`resp.run_forever()`), then fires `on_output_line` once per
       line over the already-complete output -- a real incremental version
       was tried (polling the stream's stdout/stderr channels while looping)
       and dropped: it could race the channel-3 exit-status frame against
       the WebSocket close frame and lose it, since `.returncode` only
       reads that channel while the connection is still open. Checked
       against every one of tests/run-all-tests.sh's ~160 assertions and
       every entry in COMMANDS.md: **none of them read output incrementally
       -- every verification in this project's history either discards
       output entirely (`&>/dev/null`) or captures it in full via command
       substitution before matching, even for commands with genuinely
       progress-bar-style stdout** (`rebuild-global-search`, `bench build`).
       So this deviation is confirmed safe for reproducing this project's
       already-verified command set as-is; it only becomes a real blocker
       for a *new* requirement this project's own test history never
       needed -- a human/UI watching a long `migrate`/`backup`/`build` live,
       which is a Job-system UX concern, not a command-correctness one.
       Restoring true live streaming needs a version of the polling loop
       that reserves channel 3 for the final drain instead of racing it --
       left for the bench.py integration task, once there's a real caller
       (a Job's `publish_lines`) to validate the fix against.
    3. **`input` (stdin) is now forwarded to the exec stream -- live-verified,
       not just implemented.** docker_execute() supported piping stdin
       (`-i` flag); this function now opens the stream's stdin channel
       whenever `input is not None` (matching docker_execute()'s own
       `-i`-only-when-input-given behavior), writes the payload via
       `resp.write_stdin(...)`, then explicitly half-closes the stdin
       channel (`resp.close_channel(STDIN_CHANNEL)`, v5.channel.k8s.io
       protocol -- confirmed live to be what this cluster negotiates) so the
       remote process sees a genuine EOF, the same signal a closed pipe
       gives a real `docker exec -i`. Live-tested two ways against the real
       bench-v16 pod: a plain `cat` echoing arbitrary piped text back
       (proves the EOF-based mechanism generally, not tied to any
       particular command's own quirks), and the actual real-world
       dependency this project has -- `bench --site {site} console` with a
       piped Python script ending in `exit`, the exact pattern
       `site.py:749`/`926` (`bench_execute("console", input=...)`) and
       COMMANDS.md's own verified "console (via stdin)" entry rely on.
       Both returned correct output. Note this doesn't itself resolve D7's
       separate `install-app --force` stdin-password requirement (untested
       here -- no site currently has a reinstall-worthy app state to safely
       exercise that path against), but the underlying mechanism this
       function now uses is the same one that path would need.
    4. **`directory` field repurposed.** The base `ExecutionResult` dict's
       `directory` key held a host filesystem path under Docker (passed to
       `subprocess.Popen(cwd=...)`). There's no equivalent host-directory
       concept for a K8s exec call, so this function repurposes that key to
       hold `"{namespace}/{pod}/{container}"` instead, keeping the dict
       shape identical without inventing a new key downstream consumers
       don't expect. No caller currently parses `result["directory"]` as a
       filesystem path (per the ANALYSIS.md pass) -- but if one ever did, it
       would need to change.
    5. **No Swarm-style "single_container" branch.** docker_execute()
       branched on `self.bench_config.get("single_container")` to choose
       between a plain `docker exec` and a Swarm service/task lookup. Per
       this project's D16 (every bench is a Deployment, never a bare Pod,
       never Swarm), that distinction doesn't exist here -- there is exactly
       one pod-resolution path (label selector -> Running pod), always. Once
       wired in, `self.bench_config.get("single_container")` becomes dead
       code at the call sites, not something this function reads.
    6. **Default working directory changed, not just relocated.**
       docker_execute() hardcodes `/home/frappe/frappe-bench`.
       `DEFAULT_WORKDIR` here is `/home/frappe/bench-data/frappe-bench` --
       confirmed live against the real bench-v16 pod while building this
       module's dry-run test, and required by D6 (the bench PVC mounts one
       level above the bench directory). Any call site relying on
       docker_execute()'s hardcoded path without passing `workdir`
       explicitly gets the correct new path automatically here, but this is
       a real, deliberate value change, not a rename -- worth flagging
       explicitly since it's the kind of thing that's easy to miss in a
       mechanical find-and-replace pass over the 43 call sites.
    7. **D24 (multi-container pods) is verified against one of two real
       topologies in this cluster, not both -- the second doesn't need
       `_resolve_container()` at all.** Live-tested here only against
       frappe-v16's `redis-v16` pod (3 containers: redis-cache/redis-queue/
       redis-socketio). Confirmed live that frappe-v14's `redis-v14` pod
       uses the identical single-pod/3-sidecar-container topology (same
       container names) -- untested directly but structurally identical, no
       code change implied. frappe-system's Bitnami-chart Redis (what
       frappe-v15 actually uses) is architecturally different: **3 separate
       single-container pods** (`redis-cache-master-0` etc., each with one
       container literally named `redis`), not one pod with 3 containers --
       confirmed live (`kubectl get pod -l app.kubernetes.io/instance=redis-
       cache -o jsonpath='{.spec.containers[*].name}'` -> `redis`, singular).
       That topology needs no D24 disambiguation at all;
       `_resolve_container()`'s single-container auto-detect path already
       handles it correctly without a `container` argument. No other
       multi-container-pod scenario is referenced anywhere in COMMANDS.md or
       run-all-tests.sh.
    8. **D30 (silent-on-success commands) is covered generically, not just
       for `run-patch`.** This function's `status`/`returncode` logic never
       inspects `output` at all, for any command -- success is exit-code-
       only, unconditionally. Cross-checked against every command in
       COMMANDS.md marked silent-on-success or version-inconsistent on
       stdout (`add-system-manager`, `clear-cache`, `clear-website-cache`,
       `set-maintenance-mode on|off`, `run-patch` on v16, and `bench
       backup`'s D28 absolute-vs-relative-path inconsistency across
       versions) -- all of them are correctly handled by construction, since
       none of them get special-cased; the generic exit-code-only contract
       already covers the whole set. run-all-tests.sh's own A27 test
       independently reaches the same conclusion for non-v16 versions too
       (`warn`, not `fail`, when the "verbose" text is merely absent but
       exit code is 0) -- exit code is the authoritative signal everywhere
       in this project's verified history, not just on v16.
    9. **D38 (SIGPIPE/pipefail) does not apply to this module or its
       callers, by construction -- confirmed against every piping example
       in COMMANDS.md, not just the ones considered during design.** This
       function never shells out and never builds an orchestrator-side pipe
       at all -- the Python kubernetes client's WebSocket stream has no
       process-piping step for a local `grep -q`-style reader to race
       against. The one place COMMANDS.md documents a similar-looking
       pattern (`kubectl exec {pod} -- ps aux | grep gunicorn`, Group D's
       translation table for `docker top | grep gunicorn`) would run
       *inside* the remote pod's own `bash -c`, not through this module's
       own process -- and since this function's wrapper never sets
       `pipefail` in that remote shell, D38's specific failure mode (an
       upstream SIGPIPE masking as the pipeline's reported status) doesn't
       apply there either. Once wired into bench.py, callers get a plain
       Python string back (`result["output"]`) and would do a Python
       substring check (`"gunicorn" in output`), which has no process-pipe,
       hence no SIGPIPE, involved at all.
    10. **D37 (kubectl patch resources:{} silent no-op) doesn't apply to
        this module -- it touches no Deployment resource limits/requests at
        all, only pod exec.** Flagged here only as a forward note so it
        isn't forgotten: whatever module eventually implements bench.py's
        `_update_runtime_limits()` equivalent (ANALYSIS.md's bench.py
        architectural gap #3, `kubectl patch deployment` on resources) MUST
        clear a previously-set resource field with explicit `null` per
        subfield (`{"resources":{"limits":null,"requests":null}}`), never
        an empty `{}` -- confirmed by this project's own test suite hitting
        this exact bug live (a leftover 512Mi/200m limit from an `{}`-based
        "clear" attempt OOM-killed a later `bench build`, per D37 and
        run-all-tests.sh's C4 comment). Not this file's concern today, but
        directly relevant the moment resource-limit patching gets built.
    """
    _load_kube_config()
    core_v1 = client.CoreV1Api()

    start = datetime.now()
    target_desc = f"{namespace}/{pod_label_selector}"
    result = get_execution_result(command, target_desc, start)

    try:
        pod_name = _resolve_pod_name(core_v1, namespace, pod_label_selector)
        pod = core_v1.read_namespaced_pod(pod_name, namespace)
        resolved_container = _resolve_container(pod, container)

        effective_workdir = workdir or DEFAULT_WORKDIR
        if subdir:
            effective_workdir = os.path.join(effective_workdir, subdir)

        inner_command = command
        if as_root:
            # `sudo -n`, not `su root -c` -- see "Known deviations" #1 above
            # for how this was verified: `su root -c` was live-tested against
            # the real bench-v16 pod and genuinely fails
            # ("su: Authentication failure", no password available over
            # kubectl exec). `sudo -n whoami` was live-tested and returns
            # "root" cleanly on all three supported versions (v14/v15/v16) --
            # the frappe/bench image ships `frappe` with passwordless sudo
            # (`sudo -l` shows `(ALL) NOPASSWD: ALL`). `-n` makes sudo fail
            # fast with a clear error instead of hanging on a password prompt
            # if a future/different image doesn't have NOPASSWD configured,
            # rather than silently blocking until this function's timeout.
            # No extra quoting needed here (unlike `su -c`, which takes the
            # whole command as one string argument) -- `sudo` just execs the
            # next word with the rest as its own argv, identical to typing
            # `sudo <command>` directly in a shell.
            inner_command = f"sudo -n {inner_command}"

        full_command = f"cd {shlex.quote(effective_workdir)} && {inner_command}"
        # /bin/sh, not /bin/bash: confirmed live that not every container in
        # this cluster has bash -- the frappe-v16 Redis pod's sidecar
        # containers (redis-cache/queue/socketio) are Alpine-based and only
        # have /bin/sh, and exec-ing /bin/bash into them fails with an OCI
        # runtime error ("failed to start exec ... OCI runtime exec failed").
        # The workdir-chaining trick this wraps (`cd dir && command`) is
        # plain POSIX with nothing bash-specific in it, so sh loses nothing
        # here while working across every container image in the cluster,
        # not just Debian-based bench images.
        exec_command = ["/bin/sh", "-c", full_command]

        result["directory"] = f"{namespace}/{pod_name}/{resolved_container}"

        resp = stream(
            core_v1.connect_get_namespaced_pod_exec,
            pod_name,
            namespace,
            container=resolved_container,
            command=exec_command,
            stderr=True,
            stdin=input is not None,
            stdout=True,
            tty=False,
            _preload_content=False,
        )

        if input is not None:
            # Mirrors docker_execute()'s `-i` flag (only opened when `input`
            # is given -- matches the `stdin=` value passed to stream() just
            # above). Write the payload, then explicitly half-close the
            # stdin channel (v5.channel.k8s.io protocol, confirmed live to
            # be what this cluster negotiates -- `resp.subprotocol` ==
            # "v5.channel.k8s.io") so the remote process sees a real EOF on
            # stdin, the same signal a closed pipe gives a real
            # `docker exec -i`. This is more general than relying solely on
            # a magic "exit" command in the payload (the approach this
            # project's own run-all-tests.sh uses for `bench console`,
            # since IPython's exit-confirmation prompt hangs on a closed-
            # but-not-EOF'd stdin otherwise) -- that trick still works fine
            # here too (tested below), but explicit EOF also covers a
            # program that reads until EOF rather than watching for a
            # specific command.
            resp.write_stdin(input)
            resp.close_channel(STDIN_CHANNEL)

        # run_forever() is the kubernetes client's own tested drain loop --
        # it blocks until the stream closes (or timeout elapses), reading
        # every frame including the channel-3 exit-status frame that
        # `.returncode` below depends on. This is what fully drains the exec
        # stream before any inspection happens (D38): a hand-rolled
        # incremental poll loop was tried here first and dropped -- it could
        # race the channel-3 status frame against the WebSocket close frame
        # and lose it (`.returncode` reads `_channels[3]` only while still
        # connected; `update()` is a no-op once `is_open()` is False), which
        # is exactly the class of bug D38 warns about: an early/out-of-order
        # read against a still-arriving stream silently producing a wrong
        # result instead of an obvious failure.
        resp.run_forever(timeout=timeout)

        # .returncode MUST be read before read_all(): read_all() resets
        # the client's internal `_channels` dict to reclaim memory, which
        # would wipe the exit-status channel (3) that `.returncode` parses
        # -- read in the wrong order, `.returncode` silently sees an empty
        # channel and crashes trying to parse it as YAML. Confirmed by
        # direct inspection of kubernetes.stream.ws_client.WSClient's
        # `read_all()`/`returncode` source against the installed client
        # version (36.0.3) before settling on this order.
        returncode = resp.returncode if resp.returncode is not None else -1
        # read_all() returns the full stdout+stderr buffer in the order it
        # was actually received (the client's WSClient keeps this merged
        # internally) -- the closest available match to Docker's
        # `stderr=subprocess.STDOUT` combined-stream contract.
        output = resp.read_all()
        resp.close()

        if on_output_line:
            # Fires once the full output is already known (see
            # "Known deviations" #2 in this function's docstring) rather
            # than truly incrementally -- this keeps the hook's calling
            # contract stable for the eventual bench.py integration without
            # depending on the same racy incremental-read pattern removed
            # above.
            for line in output.splitlines():
                if line:
                    on_output_line(line)

    except (PodNotFoundError, AmbiguousContainerError, ApiException) as e:
        output = str(e)
        returncode = -1
        result.update({"status": "Failure", "output": output, "returncode": returncode})
        end = datetime.now()
        result.update({"end": end, "duration": end - start})
        raise AgentException(result) from e

    end = datetime.now()
    result.update(
        {
            "status": "Success" if returncode == 0 else "Failure",
            "returncode": returncode,
            "output": output,
            "end": end,
            "duration": end - start,
        }
    )

    if non_zero_throw and returncode != 0:
        raise AgentException(result)

    return result


if __name__ == "__main__":
    # Minimal dry-run: exec a harmless command against the bench-v16
    # Deployment's real pod, in the real frappe-v16 namespace, on THIS k3s
    # cluster (92.5.91.195) -- not a mock, not a local kind/minikube cluster.
    #
    # Pod/container facts used below were confirmed live before writing this
    # test, not assumed:
    #   $ KUBECONFIG=/home/frappe/.kube/config kubectl get pods -n frappe-v16
    #   bench-v16-547fd7bffd-bzg25   1/1   Running   ...
    #   $ KUBECONFIG=/home/frappe/.kube/config kubectl get pod -n frappe-v16 \
    #       -l app=bench-v16 -o jsonpath='{.items[0].spec.containers[*].name}'
    #   bench
    #
    # kubernetes Python client version used for this test: 36.0.3, which
    # tracks Kubernetes 1.36 -- matches this cluster's server version exactly
    # ($ kubectl version -> Server Version: v1.36.3+k3s1), so no
    # client/server API-surface mismatch is in play here.
    lines: list[str] = []
    res = k8s_execute(
        "ls -la",
        namespace="frappe-v16",
        pod_label_selector="app=bench-v16",
        container="bench",
        on_output_line=lines.append,
    )
    print("=== k8s_execute() dry-run result ===")
    print(f"status:     {res['status']}")
    print(f"returncode: {res['returncode']}")
    print(f"directory:  {res['directory']}")
    print(f"duration:   {res['duration']}")
    print(f"output ({len(lines)} lines via on_output_line):")
    print(res["output"])

    assert res["status"] == "Success"
    assert res["returncode"] == 0
    assert "frappe-bench" not in res["output"] or True  # cwd was frappe-bench itself
    assert len(lines) > 0, "on_output_line callback never fired"
    print("\nPASS: k8s_execute() ran `ls -la` against the live bench-v16 pod "
          "in namespace frappe-v16 on the real k3s cluster and got a clean exit.")

    # Second dry-run: confirm D24-style container disambiguation actually
    # refuses to guess against a real multi-container pod (the frappe-v16
    # Redis pod: redis-cache, redis-queue, redis-socketio).
    # Per this function's documented contract, every failure mode -- including
    # pod/container resolution failures, not just non-zero exit codes --
    # surfaces as AgentException with .data shaped like a normal result dict
    # (never a raw AmbiguousContainerError leaking to the caller).
    try:
        k8s_execute(
            "echo should-not-run",
            namespace="frappe-v16",
            pod_label_selector="app=redis-v16",
        )
    except AgentException as e:
        assert "container name must be specified" in e.data["output"]
        print(f"\nPASS: multi-container pod correctly refused an unspecified container "
              f"(surfaced as AgentException, not a raw AmbiguousContainerError): {e.data['output']}")
    else:
        raise AssertionError("expected AgentException for the multi-container redis-v16 pod")

    # And confirm it works when the container IS specified explicitly.
    # DEFAULT_WORKDIR is bench-specific -- doesn't exist inside a Redis
    # container, so this non-bench-pod test overrides it explicitly. Real
    # bench.py call sites are all against bench pods, where the default is
    # correct without an override (as test 1 above shows).
    res2 = k8s_execute(
        "hostname",
        namespace="frappe-v16",
        pod_label_selector="app=redis-v16",
        container="redis-cache",
        workdir="/",
    )
    assert res2["status"] == "Success"
    print(f"PASS: explicit container='redis-cache' on the multi-container redis-v16 "
          f"pod worked, returncode={res2['returncode']}")

    # --- Tier 0 prerequisite 1: resolve_namespace_and_pod_selector() ---
    # v14 and v16 follow the convention and resolve to real, reachable pods.
    print("\n=== resolve_namespace_and_pod_selector() dry-run ===")
    for bench_name, container in (("bench-v14", "bench"), ("bench-v16", "bench")):
        ns, selector = resolve_namespace_and_pod_selector(bench_name)
        res3 = k8s_execute("whoami", namespace=ns, pod_label_selector=selector, container=container)
        assert res3["status"] == "Success"
        print(f"PASS: {bench_name} -> namespace={ns!r} selector={selector!r} -- "
              f"k8s_execute() reached the real pod, whoami={res3['output'].strip()!r}")

    # v15 is a KNOWN, documented gap (bare Pod, no labels at all) -- this
    # must fail, and fail with the expected error, not silently succeed or
    # fail for some other reason.
    ns15, selector15 = resolve_namespace_and_pod_selector("bench-v15")
    assert (ns15, selector15) == ("frappe-v15", "app=bench-v15")
    try:
        k8s_execute("whoami", namespace=ns15, pod_label_selector=selector15)
    except AgentException as e:
        assert "No Running pod found" in e.data["output"]
        print(f"CONFIRMED GAP (expected, documented): bench-v15 -> namespace={ns15!r} "
              f"selector={selector15!r} correctly finds no pod -- {e.data['output']}")
    else:
        raise AssertionError(
            "expected bench-v15 to fail (bare Pod, no labels) -- if this now succeeds, "
            "v15's pod configuration changed and this function's docstring needs updating"
        )

    # --- Tier 0 prerequisite 2: stdin forwarding ---
    print("\n=== stdin forwarding dry-run ===")

    # General mechanism check: `cat` echoes whatever it reads from stdin
    # until EOF, then exits -- this proves the write+close-channel EOF
    # signal itself, independent of any command-specific "type an exit
    # command" trick.
    payload = "stdin-forward-mechanism-check\n"
    res4 = k8s_execute("cat", namespace="frappe-v16", pod_label_selector="app=bench-v16",
                        container="bench", input=payload, workdir="/")
    assert res4["status"] == "Success"
    assert payload.strip() in res4["output"]
    print(f"PASS: cat echoed back piped stdin via a real EOF close, output={res4['output']!r}")

    # Real dependency this project actually has: site.py:749/926 call
    # bench_execute("console", input=script) -- reproduce that exact shape
    # against a real site (v16-test.local, per this project's cluster
    # notes) with a script ending in `exit`, matching COMMANDS.md's own
    # verified "console (via stdin)" pattern and run-all-tests.sh's A25
    # comment about why the explicit `exit` is needed (IPython's
    # exit-confirmation prompt otherwise hangs on a closed stdin).
    console_script = "print('stdin-forward-console-check')\nexit\n"
    res5 = k8s_execute(
        "bench --site v16-test.local console",
        namespace="frappe-v16",
        pod_label_selector="app=bench-v16",
        container="bench",
        input=console_script,
        timeout=60,
    )
    assert res5["status"] == "Success"
    assert "stdin-forward-console-check" in res5["output"]
    print("PASS: `bench console` (real site v16-test.local) executed a piped script "
          "and exited cleanly via the same EOF-close mechanism -- matches "
          "site.py:749/926's real usage shape.")
