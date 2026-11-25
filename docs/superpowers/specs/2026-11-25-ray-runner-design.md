# Ray runner prototype (nornir_ray): design brief

**Status**: Ready for implementation
**Source design**: "Ray Runner for Nornir — High-Level Design"
(https://claude.ai/artifact/P3yJT3KJBXJ82ZtBgDgm2o), draft of 2026-07-21.
**Goal**: build a prototype of the Ray runner in this repository to validate the
design, then move it to its own package once it is solid.

## Users and value

- Primary user: the Nornir runner designer, deciding whether the Ray design holds
  before it becomes a separate package.
- Problem today: the main claims of the design are untested: "no core edit
  required", connection reuse with one actor per host, "hard cancel", and
  placement by site. The design also targets a runner API (`run_stream`, `submit`,
  `attach`, `Execution`, `task.emit`) that differs from the one Nornir 3.7.0a1
  ships (`StreamingRunner`, `RunContext`, `PartialResult`).
- Value: a yes/no answer, backed by measurements, on serialization, connection
  reuse, the 3.7.0a1 streaming contract, overhead and placement.

## Decisions

- D1: Placement is tested with one Ray worker per site on the shared
  `sandbox-mgmt` network. Each final result records the node that ran it.
- D2: Results come back through a proxy task on the driver, which runs through
  the public `run.start_host()` / `run.astart_host()` (delegated to Nornir's
  `ThreadedRunner` / `AsyncioRunner`). Cancellation goes through
  `run.add_cancel_callback()`. Fallback: private `RunContext` calls, kept in one
  adapter module, each tied to a proposed Nornir change.
- D3: v1 implements the sync contract (`RunnerPlugin` + `StreamingRunner`) and
  the async contract (`AsyncRunnerPlugin` + `AsyncStreamingRunner`), in one
  class registered as runner `ray`.
- D4: The Ray cluster is one head node, a `site_a` worker and a `site_b` worker,
  in one Docker Compose file on `sandbox-mgmt`, outside the containerlab topology.
  The host that runs the scripts joins the cluster as a Ray node with no CPUs, so
  scripts connect as normal Ray drivers, not through Ray Client. With no address,
  the runner starts a local single-node Ray. Tasks connect to the real cEOS
  devices; simulated tasks are used for unit tests, P4 and the scale run.
- D5: Cancellation keeps the actor and its connection. Sync tasks stop at their
  next checkpoint, and async tasks stop at the call they are awaiting. A sync
  host that passes its deadline has its actor killed with `ray.kill()`; the next
  run creates a new actor and a new connection.
- D6: Code in `src/nornir_ray/`. Ray is a regular dependency, pinned
  `ray==2.60.1`, registered through the `nornir.plugins.runners` entry point.
- D7: No actor restarts and no task retries.
- D8: `cancellation_depth` is `"checkpoint"`, which is true for sync tasks.
  Async tasks stop earlier, at the awaited call. Nornir core only validates the
  value and does not act on it.

## User journeys

### P1: Stream a run across two sites

Given ceos1 and ceos2 have `data.site: site_a`, ceos3 has `site_b`, and one
worker per site is running,
when the user runs `stream_progress_sync.py` or `stream_progress_async.py` with
`--runner ray`,
then each host reports its `PartialResult` events in order and then its final
`Result`; ceos1 and ceos2 name the `site_a` worker and ceos3 names the `site_b`
worker; and the facts match the output of the local runner.

### P2: Cancellation and deadlines

Given the `cancel_on_failure_*` and `deadlines_*` scripts with slow or failing
hosts,
when the user runs them with `--runner ray`,
then each host stops at the same point as with the local runner (D5), and a sync
host past its deadline gets a new actor on the next run.

### P3: Connection reuse and overhead

Given actors already exist for the three hosts,
when the user runs two tasks in a row,
then each device shows one SSH session in `show users`, and the timing
comparison reports the extra time against the local runner.

### P4: Site with no worker

Given the `site_b` worker is stopped,
when the user runs a task on ceos3 with a per-host deadline,
then ceos3 reports a progress event that names `site_b` as having no worker, and
is then recorded as timed out.

## Functional requirements

- FR-001: Users MUST be able to select runner `ray` in the six `scripts/runner/`
  examples, with and without a cluster address (`RAY_ADDRESS`).
- FR-002: The runner MUST run each host's task on an actor placed on a worker that
  carries the Ray resource named by the host's `site` attribute.
- FR-003: The runner MUST deliver each `PartialResult` to `nr.stream()` /
  `nr.astream()` and to processors in the order the task yields it.
- FR-004: The runner MUST reuse a host's connection across runs in the same driver
  process. Nornir 3.7.0a1 binds a connection to the mode (sync or asyncio) that
  opened it until it is closed, so each host has one connection, in the mode of the
  run that opened it; switching mode needs a close first, as with local runners.
- FR-005: The runner MUST fail a run before any host starts, with an error naming
  the task, when the task or its parameters cannot be serialized.
- FR-006: The runner MUST apply cancellation and deadlines as in D5. A host whose
  site has no worker MUST get a progress event naming the site, then follow the
  normal deadline.
- FR-007: The runner MUST record a host as failed, with the node and site in the
  error, when its actor dies during a run, and MUST NOT retry it.
- FR-008: The runner MUST fail a run before any host starts, with an error that
  gives the address it tried, when the cluster address cannot be reached.
- FR-009: A task's exception and its worker-side traceback MUST appear in the
  host's `Result`.
- FR-010: The runner MUST use only public Nornir 3.7.0a1 API, except calls listed
  in one adapter module.

## Success criteria

- SC-001: P1, P2 and P4 give the same per-host outcome as the local runner in 10
  out of 10 repeated runs of each script.
- SC-002: 100% of hosts run on a worker of their own site.
- SC-003: Two runs in a row in the same mode open exactly one connection per host.
- SC-004: A warm run on the three lab devices takes at most 100 ms longer than
  the local runner.
- SC-005: A cold run takes at most 3 s longer than the local runner.
- SC-006 (measured, not pass/fail): per-host overhead and memory of a simulated
  task on up to 500 hosts across two sites, measured at 50, 100, 200 and 500
  hosts.
- Verdict: failing SC-001 to SC-003 rejects the actor-per-host design; failing
  SC-004 or SC-005 means the overhead needs work before the move.

## Governance gates

- New dependency: `ray==2.60.1`, regular dependency.
- AGENTS.md update: `src/nornir_ray/`, the Ray Compose file and invoke tasks, the
  D5 cancellation rules.
- Nornir change: none planned; any adapter call becomes a proposal.
- Database, API, auth, CI/CD: not touched.

## Assumptions

- The containerlab lab is running, and the host can reach `sandbox-mgmt`
  (172.24.100.0/24), whose gateway address on the host is 172.24.100.1.
- Ray 2.60.1 supports Python 3.12, the driver's Python, and the worker image uses
  the same Python minor version and the same locked packages.
- Lab credentials reach the workers through the inventory sent with each host,
  which is acceptable for a lab.
- cloudpickle serializes the task functions defined in `scripts/runner/`.

## Findings for the design (report back)

- The runner API in the design differs from the one in Nornir 3.7.0a1.
- 3.7.0a1 has no public method to record a result produced in another process;
  D2 tests whether a proxy task makes one unnecessary.
- "Hard cancel" does not hold for sync tasks on a per-host actor.
- 3.7.0a1 has no detach or re-attach API.
- A Ray actor is one OS process, so one actor per host costs one process per
  host; SC-006 measures where this stops fitting on one machine.
- Nornir 3.7.0a1 binds a connection name to one mode, so "one connection per host
  per mode" is not possible; a host's actor holds one connection at a time.
- Under `uv run`, Ray 2.60.1 copies the project to each worker and starts it with
  `uv run`; concurrent workers race on creating that copy's virtual environment.
  `nornir_ray` turns this off (`RAY_ENABLE_UV_RUN_RUNTIME_ENV=0`).
- Exceptions whose constructor takes arguments not kept in `args` (Nornir's own,
  among others) do not survive pickling; the runner rebuilds them on the driver.
- A Ray driver must run on a Ray node (or use Ray Client), so "the driver stays
  thin" still requires the driver machine to join the cluster.

## Out of scope (v1)

- Detach and re-attach of runs.
- A separate management network per site.
- Ray Client (`ray://`) and Ray Jobs.
- Ray nodes on more than one machine.
- Actor pools and eviction of idle actors.
- Retries and actor restarts.
- Security review of credential distribution to workers.
- Moving `nornir_ray` to its own repository.
