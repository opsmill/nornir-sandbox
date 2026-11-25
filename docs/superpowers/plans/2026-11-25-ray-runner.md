# Ray Runner Prototype Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `nornir_ray`, a Nornir 3.7.0a1 runner that runs each host's task on a Ray actor placed by site, and use it from the six `scripts/runner/` examples to validate the Ray runner design.

**Architecture:** On the driver, `RayRunner` wraps the user's task in a proxy task and hands it to Nornir's own `ThreadedRunner` (sync) or `AsyncioRunner` (async), so Nornir core records results, partial results, cancellation and deadlines through public API only. Each proxy sends the real task to a long-lived `HostActor` (one per host, placed with the host's `site` as a Ray custom resource) and relays the actor's streamed events back as `PartialResult`s and a final `Result`. The actor rebuilds a one-host Nornir on the worker, keeps the `Host` object (and therefore its scrapli connections) across calls, and runs the task through a local `RunContext` so checkpoints and cancellation work on the worker too.

**Tech Stack:** Python 3.12, nornir 3.7.0a1, nornir-scrapli 1.6.0a1, ray 2.60.1, cloudpickle (bundled with Ray), pytest, mypy --strict, ruff, Docker Compose, containerlab.

**Spec:** `docs/superpowers/specs/2026-11-25-ray-runner-design.md`

## Global Constraints

- Pin `ray==2.60.1` with `==` in `pyproject.toml` `dependencies`; keep `nornir==3.7.0a1` and `nornir-scrapli[asyncio]==1.6.0a1` unchanged.
- Runner plugin name: `ray`, entry point `[project.entry-points."nornir.plugins.runners"] ray = "nornir_ray:RayRunner"`.
- Package path: `src/nornir_ray/`, added to `[tool.hatch.build.targets.wheel] packages`.
- Site attribute: `site` (inventory `data.site`, read with `host.get("site")`); its value is the Ray custom resource name (`site_a`, `site_b`).
- Cluster address: `RayRunner(address=...)`, falling back to the `RAY_ADDRESS` environment variable; neither set means a local single-node Ray.
- `cancellation_depth = "checkpoint"`, `supports_background = True`.
- No actor restarts (`max_restarts=0`), no task retries (`max_task_retries=0`).
- Only public Nornir API outside `src/nornir_ray/_nornir_adapter.py`. The plan uses none; create that module only if a step proves a private call is needed, with a comment naming the proposed Nornir change.
- `uv run invoke lint` (ruff check, ruff format --check, mypy --strict) and `uv run invoke test` pass after every task.
- Unit tests never need the lab; they use a local Ray started by a session fixture.
- Lab addresses: Ray head 172.24.100.21, `site_a` worker 172.24.100.31, `site_b` worker 172.24.100.32, host gateway 172.24.100.1, Ray port 6379.

## Review Focus

1. **Host with no `site`.** Expect the actor to be placed on any node and `ray_site` to be `None`, not a scheduling error. Test: Task 4, `test_host_without_site_runs_anywhere`.
2. **Driver host that already holds an open connection** (a threaded run happened earlier in the same process). Expect `detach_host()` to drop connections so pickling works. Test: Task 2, `test_detach_host_drops_connections`.
3. **A run right after a cancelled run on the same host.** Expect the second call to wait until the first finishes on the actor, not to interleave on the same connection. Test: Task 3, `test_calls_on_one_actor_do_not_overlap`.
4. **Task parameters that can't be serialized** (the function is fine, a parameter is not). Expect `TaskNotSerializableError` naming the task. Test: Task 4, `test_unserializable_params_fail_before_any_host_starts`.
5. **Closing connections at the end of a script, and `close_failed_connections()` with Ray.** `with nr:` / `async with nr:` run `close_connections_task` through the runner. Expect it to close the worker connections and return without error. Tests: Task 4, `test_context_exit_closes_worker_connections`; Task 6, `test_close_failed_connections_runs_through_the_runner`.

---

## File structure

| File | Responsibility |
| --- | --- |
| `src/nornir_ray/__init__.py` | Exports `RayRunner` and the error classes. |
| `src/nornir_ray/errors.py` | `TaskNotSerializableError`, `RayClusterUnreachableError`, `HostActorDiedError`, `RemoteTaskError`. |
| `src/nornir_ray/wire.py` | Plain, picklable data sent between driver and actor, plus conversions to and from Nornir objects. No Ray import. |
| `src/nornir_ray/worker.py` | `HostActor`: rebuilds a one-host Nornir and runs sync and async tasks, streaming events. |
| `src/nornir_ray/cluster.py` | Connecting to Ray, checking an address, site resources, `ActorPool` (one actor per host). |
| `src/nornir_ray/runner.py` | `RayRunner` and the sync and async proxy tasks. |
| `tests/nornir_ray/conftest.py` | Session fixture: local Ray with `site_a` and `site_b` resources. |
| `tests/nornir_ray/test_*.py` | Unit tests per module. |
| `lab/ray/Dockerfile`, `lab/ray/compose.yml` | Ray head and site workers on `sandbox-mgmt`. |
| `scripts/runner/benchmark_ray.py`, `scripts/runner/count_sessions.py` | SC-003 to SC-006 measurements. |

---

### Task 1: Dependency, package skeleton and Ray behaviour checks

Pins Ray and proves, on the pinned version, the four Ray behaviours the design relies on. If one of these tests fails, stop and report: the design depends on it.

**Files:**
- Modify: `pyproject.toml` (dependencies, entry point, hatch packages, mypy override, pytest `testpaths` unchanged)
- Create: `src/nornir_ray/__init__.py` (empty `__all__` for now)
- Create: `tests/nornir_ray/__init__.py`, `tests/nornir_ray/conftest.py`
- Test: `tests/nornir_ray/test_ray_behaviour.py`

**Interfaces:**
- Produces: pytest fixture `ray_local` (session scope) that calls `ray.init(num_cpus=4, resources={"site_a": 100, "site_b": 100}, include_dashboard=False, log_to_driver=False)` once if `not ray.is_initialized()`, yields, and calls `ray.shutdown()` at the end.

- [ ] **Step 1: Add the dependency and the plugin registration**

In `pyproject.toml`: add `"ray==2.60.1"` to `dependencies`; add `[project.entry-points."nornir.plugins.runners"]` with `ray = "nornir_ray:RayRunner"`; set hatch `packages = ["src/nornir_sandbox", "src/nornir_ray"]`; add a mypy override `module = ["ray", "ray.*"]` with `ignore_missing_imports = true` and `follow_imports = "skip"`. Run `uv sync`.

- [ ] **Step 2: Write the Ray behaviour tests**

```python
# tests/nornir_ray/test_ray_behaviour.py
def test_async_actor_streams_generator_items(ray_local): ...
    # an async actor method that is an async generator yielding 1, 2, 3;
    # iterating the ObjectRefGenerator with ray.get(next(gen)) gives [1, 2, 3]
def test_wait_on_streaming_generator_times_out_then_returns(ray_local): ...
    # generator sleeps 1 s before its first item: ray.wait([gen], timeout=0.1) returns ([], [gen]);
    # ray.wait([gen], timeout=5) returns ([gen], [])
def test_cancel_reaches_running_async_actor_call(ray_local): ...
    # a second method call (cancel) runs while the first call awaits asyncio.sleep(30)
    # and sets an asyncio.Event; the first call returns within 2 s
def test_custom_resource_places_actor(ray_local): ...
    # Actor.options(resources={"site_b": 0.01}).remote() reports, via
    # ray.get_runtime_context().get_assigned_resources(), a "site_b" entry
def test_missing_resource_keeps_actor_pending(ray_local): ...
    # Actor.options(resources={"site_c": 0.01}).remote(); ray.wait([actor.ping.remote()], timeout=2)
    # returns ([], [ref]); "site_c" not in ray.cluster_resources()
```

- [ ] **Step 3: Run them**

Run: `uv run pytest tests/nornir_ray/test_ray_behaviour.py -v`
Expected: 5 passed. If one fails, record the behaviour of Ray 2.60.1 in the commit message and stop for a decision.

- [ ] **Step 4: Lint and commit**

Run: `uv run invoke lint` and expect no errors.

```bash
git add pyproject.toml uv.lock src/nornir_ray tests/nornir_ray
git commit -m "feat(nornir_ray): pin ray 2.60.1 and check the Ray behaviours the runner relies on"
```

---

### Task 2: Data sent between driver and actor (`wire.py`, `errors.py`)

**Files:**
- Create: `src/nornir_ray/errors.py`, `src/nornir_ray/wire.py`
- Test: `tests/nornir_ray/test_wire.py`

**Interfaces:**
- Produces (`errors.py`):
  - `class TaskNotSerializableError(Exception)`: `__init__(self, task_name: str, reason: str)`, message `f"task {task_name!r} cannot be sent to Ray workers: {reason}"`.
  - `class RayClusterUnreachableError(Exception)`: `__init__(self, address: str, reason: str)`, message contains `address`.
  - `class HostActorDiedError(Exception)`: `__init__(self, host: str, node: str | None, site: str | None, reason: str)`, message names host, node and site.
  - `class RemoteTaskError(Exception)`: `__init__(self, type_name: str, message: str)`; stands in for a worker exception that can't be pickled.
- Produces (`wire.py`), all frozen dataclasses:
  - `TaskCall(fn: Callable[..., Any], params: dict[str, Any], name: str, severity_level: int, global_dry_run: bool, config: Config)`.
  - `WireResult(name: str | None, result: Any, changed: bool, diff: str, failed: bool, exception: BaseException | None, severity_level: int)`.
  - `WirePartial(result: Any, progress: float | None)`.
  - `WireFinal(results: list[WireResult], node: str, site: str | None)`; `results[0]` is the top-level result.
  - `WireEvent = WirePartial | WireFinal`.
  - `detach_host(host: Host) -> Host`: shallow copy (`copy.copy`) with `connections`, `_async_connections`, `_capability_connections` and `_opening` set to new empty dicts. Note: these are slot attributes of `Host`; this is public-by-convention state, not a `RunContext` call, so it stays here.
  - `ensure_serializable(call: TaskCall) -> None`: `cloudpickle.dumps(call.fn)` and `cloudpickle.dumps(call.params)`; on any exception raise `TaskNotSerializableError(call.name, str(error))`.
  - `to_wire_result(result: Result) -> WireResult`: keeps the exception if `pickle.dumps(exception)` works, else replaces it with `RemoteTaskError(type(exc).__name__, str(exc))`.
  - `from_wire_result(wire: WireResult, host: Host, **extra: Any) -> Result` (`extra` becomes result attributes, used for `ray_node` and `ray_site`).
  - `to_wire_partial(partial: PartialResult) -> WirePartial`; `from_wire_partial(wire: WirePartial, host: Host) -> PartialResult`.

- [ ] **Step 1: Write the failing tests**

```python
def test_detach_host_drops_connections(): ...
    # host from init_nornir_sync() with a fake object in host.connections["scrapli"];
    # detached = detach_host(host); detached.connections == {}; host.connections still has it;
    # pickle.loads(cloudpickle.dumps(detached)).hostname == host.hostname
def test_detached_host_keeps_group_and_default_values(): ...
    # detach_host(ceos1).platform == "arista_eos" (comes from group ceos)
def test_config_round_trips(): ...
    # cloudpickle round-trip of init_nornir_sync().config keeps .runner.plugin
def test_ensure_serializable_rejects_unpicklable_function(): ...
    # fn closes over a threading.Lock -> TaskNotSerializableError with the task name in str()
def test_ensure_serializable_rejects_unpicklable_params(): ...
    # params={"lock": threading.Lock()} -> TaskNotSerializableError
def test_result_round_trip_keeps_fields_and_extra_attributes(): ...
    # Result(changed=True, diff="d", failed=True, exception=ValueError("boom"), result="tb") ->
    # from_wire_result(..., ray_node="n1", ray_site="site_a"): same fields, .ray_node == "n1"
def test_unpicklable_exception_becomes_remote_task_error(): ...
    # exception class holding a lock -> RemoteTaskError whose str() has the type name and message
def test_partial_round_trip(): ...
```

- [ ] **Step 2: Run them and see them fail**

Run: `uv run pytest tests/nornir_ray/test_wire.py -v`
Expected: FAIL with `ModuleNotFoundError: nornir_ray.wire`.

- [ ] **Step 3: Implement `errors.py` and `wire.py`** with the signatures above.

- [ ] **Step 4: Run the tests**

Run: `uv run pytest tests/nornir_ray/test_wire.py -v`
Expected: 8 passed.

- [ ] **Step 5: Lint and commit**

```bash
git add src/nornir_ray/errors.py src/nornir_ray/wire.py tests/nornir_ray/test_wire.py
git commit -m "feat(nornir_ray): add the data sent between driver and Ray actors"
```

---

### Task 3: Host actor (`worker.py`)

**Files:**
- Create: `src/nornir_ray/worker.py`
- Test: `tests/nornir_ray/test_worker.py`

**Interfaces:**
- Consumes: `TaskCall`, `WirePartial`, `WireFinal`, `to_wire_result`, `to_wire_partial` (Task 2).
- Produces: `HostActor`, a `@ray.remote` async actor class (the undecorated class is `HostActorImpl`, so it can be type-checked and unit-tested without Ray):
  - `__init__(self, host: Host, defaults: Defaults, site: str | None)`: stores the host; builds nothing yet.
  - `async def run(self, call: TaskCall) -> AsyncIterator[WireEvent]`: streaming method. Holds an `asyncio.Lock` for the whole call. Builds (once per `config` identity) a one-host `Nornir(inventory=Inventory(hosts=Hosts({name: host}), groups=..., defaults=...), config=call.config)`; creates `Task(call.fn, nornir, call.global_dry_run, Processors([collector]), call.name, call.severity_level, **call.params)` and a local `RunContext(task, [host])`. If `call.fn` is a coroutine function or async generator function, it runs `run.astart_host(task, host)` as an `asyncio.Task` on the actor loop; otherwise it runs `run.start_host` in `asyncio.to_thread`. The collector processor forwards top-level `task_instance_progress` events (`task.parent_task is None`) to an `asyncio.Queue` with `loop.call_soon_threadsafe`. Yields each `WirePartial` as it arrives, then one `WireFinal(results=[to_wire_result(r) for r in multi], node=self.node_name(), site=site)`.
  - `async def cancel(self) -> None`: calls `cancel()` on the current local `RunContext`; for an async call also cancels its `asyncio.Task`. No effect when idle.
  - `def node_name(self) -> str`: `ray.get_runtime_context().get_node_id()` resolved to the node's `NodeName` from `ray.nodes()`, cached; falls back to the node id.
  - `async def open_connections(self) -> dict[str, int]`: `{"sync": len(host.connections), "async": len(host._async_connections)}`, for tests and `count_sessions.py`.

- [ ] **Step 1: Write the failing tests** (they use `HostActor` through Ray with `ray_local`, and simulated tasks)

```python
def test_sync_generator_task_streams_partials_then_final(ray_local): ...
    # task yields PartialResult(task.host, "a", progress=0.5), then returns Result(result="done");
    # events == [WirePartial("a", 0.5), WireFinal(...results[0].result == "done")]
def test_async_generator_task_streams_partials_then_final(ray_local): ...
def test_host_state_persists_across_calls(ray_local): ...
    # task increments task.host.data["calls"] and returns it: 1 then 2
def test_task_exception_keeps_worker_traceback(ray_local): ...
    # task raises ValueError("boom"): final results[0].failed, exception is ValueError,
    # "Traceback" in result and "boom" in result
def test_cancel_stops_sync_task_at_next_checkpoint(ray_local): ...
    # sync generator yields every 0.2 s for 10 s; cancel after first event; final within 1 s,
    # exception is TaskCancelledError
def test_cancel_stops_async_task_at_awaited_call(ray_local): ...
    # async task awaits asyncio.sleep(30); cancel; final within 1 s, TaskCancelledError
def test_calls_on_one_actor_do_not_overlap(ray_local): ...
    # two run() calls started together on one actor; each records (start, end) in host.data;
    # the intervals do not overlap
def test_final_names_node_and_site(ray_local): ...
    # actor created with site="site_a": WireFinal.site == "site_a" and node is a non-empty str
```

- [ ] **Step 2: Run them and see them fail**

Run: `uv run pytest tests/nornir_ray/test_worker.py -v`
Expected: FAIL with `ModuleNotFoundError: nornir_ray.worker`.

- [ ] **Step 3: Implement `worker.py`** with the interface above.

- [ ] **Step 4: Run the tests**

Run: `uv run pytest tests/nornir_ray/test_worker.py -v`
Expected: 8 passed.

- [ ] **Step 5: Lint and commit**

```bash
git add src/nornir_ray/worker.py tests/nornir_ray/test_worker.py
git commit -m "feat(nornir_ray): add the per-host Ray actor that runs sync and async tasks"
```

---

### Task 4: Sync runner (`cluster.py`, `runner.py`: `run` and `execute`)

**Files:**
- Create: `src/nornir_ray/cluster.py`, `src/nornir_ray/runner.py`
- Modify: `src/nornir_ray/__init__.py` (export `RayRunner` and the errors)
- Test: `tests/nornir_ray/test_runner_sync.py`

**Interfaces:**
- Consumes: Tasks 2 and 3.
- Produces (`cluster.py`):
  - `resolve_address(address: str | None) -> str | None`: `address or os environ RAY_ADDRESS or None`.
  - `check_reachable(address: str, timeout: float = 5.0) -> None`: TCP connect to `host:port`; on failure raise `RayClusterUnreachableError(address, reason)`.
  - `connect(address: str | None, resources: dict[str, float] | None) -> None`: no-op if `ray.is_initialized()`; with an address, `check_reachable` then `ray.init(address=address)`; without, `ray.init(resources=resources)`.
  - `site_available(site: str) -> bool`: `site in ray.cluster_resources()`.
  - `class ActorPool`: `get(self, host: Host, site: str | None) -> ActorHandle` (creates `HostActor.options(resources={site: 0.01} if site else {}, num_cpus=0, max_restarts=0, max_task_retries=0).remote(detach_host(host), defaults, site)` on first use, then reuses it); `kill(self, host_name: str) -> None` (`ray.kill`, forget the handle); `actor_id(self, host_name: str) -> str | None`.
- Produces (`runner.py`):
  - `class RayRunner` with class attributes `cancellation_depth = "checkpoint"`, `supports_background = True`, `run_options = RunOptions()`.
  - `__init__(self, address: str | None = None, num_workers: int = 20, per_host_timeout: float | None = None, timeout: float | None = None, site_key: str = "site", resources: dict[str, float] | None = None)`. `resources` is only used for a local Ray (tests declare sites with it).
  - `pool: ActorPool` (public, used by tests and `count_sessions.py`).
  - `run(self, task: Task, hosts: list[Host]) -> AggregatedResult`: same shape as `ThreadedRunner.run`.
  - `execute(self, task: Task, hosts: list[Host], run: RunContext) -> Iterator[None]`: `connect(...)`, `ensure_serializable(...)` (both before any host starts), builds the proxy task with `Task(proxy, task.nornir, task.global_dry_run, task.processors, task.name, task.severity_level)`, then `yield from ThreadedRunner(self.num_workers).execute(proxy_task, hosts, run)`. Registers `run.add_cancel_callback` to call `actor.cancel.remote()` for every host in `run.running_hosts()`.
  - Sync proxy, a generator task `proxy(task: Task) -> Generator[PartialResult, None, Result]`:
    1. `site = task.host.get(site_key)`; if `site` and not `site_available(site)`, yield `PartialResult(task.host, f"no Ray worker for site {site}; waiting")`.
    2. `gen = actor.run.remote(call)`; loop on `ray.wait([gen], timeout=0.1)`. On each empty wait, check `task.host not in run.running_hosts()`: the host was recorded by a deadline, so call `pool.kill(host.name)` (D5) and return the recorded `Result` (Nornir drops it, the record already exists).
    3. For each `WirePartial`, yield `from_wire_partial(...)`. A `TaskInterruptedError` thrown into the proxy at that `yield` sends `actor.cancel.remote()` and re-raises.
    4. On `WireFinal`, append `results[1:]` to `task.results` as subtask results, and return `from_wire_result(results[0], task.host, ray_node=final.node, ray_site=final.site)`.
    5. On `ray.exceptions.ActorDiedError` (or `RayActorError`), forget the actor and raise `HostActorDiedError(host.name, node, site, str(error))`.

- [ ] **Step 1: Write the failing tests** (simulated tasks; inventory from `init_nornir_sync(runner="threaded")` then `nr.with_runner(RayRunner(resources=...))`; a `tmp_path` inventory with sites for the site tests)

```python
def test_run_returns_results_for_every_host(ray_local): ...
def test_stream_events_match_threaded_runner(ray_local): ...
    # same generator task on runner "ray" and on ThreadedRunner: per host, the list of
    # (type, result, progress) from nr.stream() is identical (FR-003)
def test_hosts_run_on_their_site(ray_local): ...
    # hosts with data.site site_a/site_b: result[0].ray_site equals host data.site (FR-002, SC-002)
def test_host_without_site_runs_anywhere(ray_local): ...
    # ray_site is None, result not failed
def test_unserializable_task_fails_before_any_host_starts(ray_local): ...
    # pytest.raises(TaskNotSerializableError, match=task name); a processor saw no task_instance_started
def test_unserializable_params_fail_before_any_host_starts(ray_local): ...
def test_unreachable_address_fails_with_the_address(): ...
    # RayRunner(address="127.0.0.1:1") in a subprocess-free check: call
    # cluster.check_reachable("127.0.0.1:1", timeout=1) -> RayClusterUnreachableError("127.0.0.1:1" in str)
def test_task_exception_shows_worker_traceback(ray_local): ... (FR-009)
def test_actor_reused_across_runs(ray_local): ...
    # two nr.run() calls: pool.actor_id(host) is the same; host.data counter task returns 1 then 2 (FR-004)
def test_cancel_keeps_actor(ray_local): ...
    # nr.submit() of a slow generator task, handle.cancel(): TaskCancelledError; actor_id unchanged (D5)
def test_deadline_in_blocking_call_kills_actor(ray_local): ...
    # per_host_timeout=1, task time.sleep(5) with no checkpoint: TaskTimeoutError within 2 s;
    # next run succeeds and actor_id changed (D5)
def test_missing_site_reports_progress_then_times_out(ray_local): ...
    # data.site site_c, per_host_timeout=2: stream has a PartialResult containing "site_c",
    # final exception is TaskTimeoutError (FR-006, P4)
def test_actor_death_fails_host_without_retry(ray_local): ...
    # task calls os._exit(1): HostActorDiedError naming host and site; it ran once (FR-007, D7)
def test_context_exit_closes_worker_connections(ray_local): ...
    # with nr: run a task that puts a fake connection in host.connections; after exit,
    # actor.open_connections() == {"sync": 0, "async": 0}
```

- [ ] **Step 2: Run them and see them fail**

Run: `uv run pytest tests/nornir_ray/test_runner_sync.py -v`
Expected: FAIL with `ModuleNotFoundError: nornir_ray.runner`.

- [ ] **Step 3: Implement `cluster.py`, `runner.py` (sync part) and the exports.**

- [ ] **Step 4: Run the tests**

Run: `uv run pytest tests/nornir_ray/test_runner_sync.py -v`
Expected: 14 passed. Then `uv run invoke test` passes in full.

- [ ] **Step 5: Lint and commit**

```bash
git add src/nornir_ray tests/nornir_ray/test_runner_sync.py
git commit -m "feat(nornir_ray): add RayRunner for sync tasks with placement by site"
```

---

### Task 5: Async runner (`arun`, `aexecute`)

**Files:**
- Modify: `src/nornir_ray/runner.py`
- Test: `tests/nornir_ray/test_runner_async.py`

**Interfaces:**
- Consumes: `RayRunner`, `ActorPool`, `connect`, wire functions (Task 4).
- Produces:
  - `async def arun(self, task: Task, hosts: list[Host]) -> AggregatedResult`: same shape as `AsyncioRunner.arun`.
  - `async def aexecute(self, task: Task, hosts: list[Host], run: RunContext) -> None`: `await asyncio.to_thread(connect, ...)`, `ensure_serializable(...)`, then `await AsyncioRunner(self.num_workers).aexecute(proxy_task, hosts, run)`.
  - Async proxy, an async generator task `aproxy(task: Task) -> AsyncGenerator[PartialResult | Result, None]`: same steps 1, 3, 4 and 5 as the sync proxy, iterating `async for ref in actor.run.remote(call)` and `await ref`. On `asyncio.CancelledError`, `await actor.cancel.remote()` and re-raise (D5: the actor is kept). No kill on deadline: `AsyncioRunner` cancels the host's coroutine.

- [ ] **Step 1: Write the failing tests** (mirror Task 4 for async tasks)

```python
async def test_arun_returns_results_for_every_host(ray_local): ...
async def test_astream_events_match_asyncio_runner(ray_local): ...
async def test_async_hosts_run_on_their_site(ray_local): ...
async def test_async_cancel_stops_at_awaited_call_and_keeps_actor(ray_local): ...
    # asubmit of a task awaiting asyncio.sleep(30); cancel: TaskCancelledError within 1 s; actor_id unchanged
async def test_async_deadline_keeps_actor(ray_local): ...
    # per_host_timeout=1: TaskTimeoutError within 2 s; actor_id unchanged
async def test_async_missing_site_reports_progress_then_times_out(ray_local): ...
async def test_async_context_exit_closes_worker_connections(ray_local): ...
async def test_sync_and_async_connections_counted_per_mode(ray_local): ...
    # a sync run then an async run on one host, each opening a fake connection in its mode:
    # actor.open_connections() == {"sync": 1, "async": 1}
```

Use `pytest.mark.asyncio` only if `pytest-asyncio` is added; otherwise wrap each body in `asyncio.run(...)` from a plain test, as `tests/test_inventory.py` does. Prefer `asyncio.run` (no new dev dependency).

- [ ] **Step 2: Run them and see them fail**

Run: `uv run pytest tests/nornir_ray/test_runner_async.py -v`
Expected: FAIL with `RunnerNotAsyncError` or `AttributeError: arun`.

- [ ] **Step 3: Implement `arun`, `aexecute` and the async proxy.**

- [ ] **Step 4: Run the tests**

Run: `uv run pytest tests/nornir_ray -v`
Expected: all passed.

- [ ] **Step 5: Lint and commit**

```bash
git add src/nornir_ray/runner.py tests/nornir_ray/test_runner_async.py
git commit -m "feat(nornir_ray): run async tasks on Ray actors"
```

---

### Task 6: Use the Ray runner from the sandbox scripts

**Files:**
- Modify: `src/nornir_sandbox/core.py` (`RunnerName`, `_settings`, `init_nornir`, `close_failed_connections`)
- Modify: `src/nornir_sandbox/cli.py` (`SyncRunner`, new `AsyncRunner`, `format_event`)
- Modify: `inventory/hosts.yaml` (`data.site`: ceos1 and ceos2 `site_a`, ceos3 `site_b`)
- Modify: the six `scripts/runner/*.py` scripts
- Test: `tests/test_inventory.py`

**Interfaces:**
- Consumes: runner plugin `ray` (Task 1 entry point; Tasks 4 and 5).
- Produces:
  - `RunnerName = Literal["asyncio", "threaded", "serial", "ray"]`.
  - `init_nornir(..., runner: Literal["asyncio", "ray"] = "asyncio")`; `init_nornir_sync(..., runner: RunnerName = "asyncio")` accepts `"ray"`. For `"ray"`, runner options are `num_workers`, `per_host_timeout`, `timeout`; the address comes from `RAY_ADDRESS`.
  - `close_failed_connections(nr, result)` closes the failed hosts through the runner: `await nr.filter(filter_func=lambda h: h.name in failed).aclose_connections(on_good=True, on_failed=True)`, so it also closes connections held by Ray actors.
  - `SyncRunner = Literal["threaded", "serial", "ray"]`; `AsyncRunner = Annotated[Literal["asyncio", "ray"], Parameter(help=...)]`.
  - `format_event()` appends `  [node <ray_node>]` to a final `Result` that has a `ray_node` attribute.
  - Each `*_async.py` script gets `runner: AsyncRunner = "asyncio"` passed to `init_nornir`. Each `*_sync.py` script's docstring says that `ray` runs hosts on Ray actors (D5 rules).

- [ ] **Step 1: Write the failing tests**

```python
def test_hosts_have_a_site(): ...
    # every host in inventory has data.site in {"site_a", "site_b"}
@pytest.mark.parametrize("runner", ["threaded", "serial", "ray"])  # extend the existing test
def test_sync_init_selects_the_runner_and_its_deadlines(runner, expected): ...  # "ray" -> "RayRunner"
def test_async_init_selects_the_ray_runner(): ...
    # type(asyncio.run(init_nornir(runner="ray")).runner).__name__ == "RayRunner"
def test_close_failed_connections_runs_through_the_runner(): ...
    # with a recording runner stub implementing arun: the stub receives one run
    # whose hosts are exactly the failed ones
def test_format_event_names_the_node(): ...
    # Result with ray_node="ray-site-a" -> line ends with "[node ray-site-a]"
```

- [ ] **Step 2: Run them and see them fail**

Run: `uv run pytest tests/test_inventory.py -v`
Expected: the new tests FAIL.

- [ ] **Step 3: Implement the changes**, then run each script once with a local Ray (no `RAY_ADDRESS`) and the lab up: `uv run python scripts/runner/stream_progress_sync.py --runner ray` and the five others with their defaults plus `--runner ray`.

- [ ] **Step 4: Run the tests**

Run: `uv run invoke test` and `uv run invoke lint`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/nornir_sandbox inventory/hosts.yaml scripts/runner tests/test_inventory.py
git commit -m "feat: run the runner examples on Ray with --runner ray"
```

---

### Task 7: Ray cluster on the lab network

**Files:**
- Create: `lab/ray/Dockerfile`, `lab/ray/compose.yml`
- Modify: `tasks.py` (tasks `ray-up`, `ray-down`, `ray-status`), `AGENTS.md`, `README.md`

**Interfaces:**
- Produces:
  - Image `nornir-sandbox-ray`: `python:3.12-slim`, `uv` copied from `ghcr.io/astral-sh/uv`, the repository copied in, `uv sync --frozen --no-dev`, `PATH` includes `.venv/bin`. The build context is the repository root (`compose.yml` sets `context: ../..`).
  - Services on the external network `sandbox-mgmt`:
    - `ray-head` at 172.24.100.21: `ray start --head --port=6379 --num-cpus=0 --node-name=ray-head --block`.
    - `ray-site-a` at 172.24.100.31: `ray start --address=172.24.100.21:6379 --resources='{"site_a": 100}' --node-name=ray-site-a --block`.
    - `ray-site-b` at 172.24.100.32: same with `site_b` and `ray-site-b`.
  - `invoke ray-up`: `docker compose -f lab/ray/compose.yml up -d --build`, then joins the host as a node with `uv run ray start --address=172.24.100.21:6379 --num-cpus=0 --node-ip-address=172.24.100.1`, then prints `export RAY_ADDRESS=172.24.100.21:6379`.
  - `invoke ray-down`: `uv run ray stop`, then `docker compose ... down`.
  - `invoke ray-status`: `uv run ray status --address=172.24.100.21:6379`.

- [ ] **Step 1: Write the Dockerfile and Compose file.** Check that the image has the same Python minor version and Ray version as the driver: `docker run --rm nornir-sandbox-ray python -c "import sys, ray; print(sys.version_info[:2], ray.__version__)"` prints `(3, 12) 2.60.1`.

- [ ] **Step 2: Add the invoke tasks**, then run `uv run invoke up` (if the lab is down) and `uv run invoke ray-up`.

- [ ] **Step 3: Check the cluster**

Run: `uv run invoke ray-status`
Expected: 4 nodes, with resources `site_a: 100` and `site_b: 100`.

- [ ] **Step 4: Check placement end to end**

Run: `RAY_ADDRESS=172.24.100.21:6379 uv run python scripts/runner/stream_progress_sync.py --runner ray`
Expected: ceos1 and ceos2 final lines end with `[node ray-site-a]`, ceos3 with `[node ray-site-b]`.

- [ ] **Step 5: Document and commit.** Update AGENTS.md (key directories: `src/nornir_ray/`, `lab/ray/`; commands: `ray-up`, `ray-down`, `ray-status`; conventions: the D5 rules and "`RAY_ADDRESS` selects the cluster; unset means a local Ray") and add a README section on the Ray runner.

```bash
git add lab/ray tasks.py AGENTS.md README.md
git commit -m "feat: add a Ray cluster with one worker per site on the lab network"
```

---

### Task 8: Measurements and validation report

**Files:**
- Create: `scripts/runner/count_sessions.py`, `scripts/runner/benchmark_ray.py`
- Create: `docs/superpowers/reports/2026-11-25-ray-runner-validation.md`

**Interfaces:**
- Produces:
  - `count_sessions.py` (cyclopts, sync): runs `show users | json` on each host with the threaded runner and prints the number of SSH sessions per host. Used for SC-003: run `stream_progress_sync.py --runner ray` and `stream_progress_async.py --runner ray` twice in one process (add a `--repeat 2` option to both streaming scripts in this task), then compare `count_sessions.py` before and after.
  - `benchmark_ray.py` (cyclopts, sync): options `--hosts-count` (default 3, lab), `--simulated` (generate an inventory in a temp directory with N hosts split across `site_a` and `site_b`, and run a task that sleeps 0.05 s and yields two partial results), `--repeat` (default 10). Prints, for runner `ray` and for the local runner, the cold-run time, the median warm-run time, the extra time per host, and the driver plus Ray process RSS (`psutil` is not a dependency: read `ray.cluster_resources()` / `ray memory --stats-only` output, or `ps -o rss` of `raylet` and `ray::` processes).

- [ ] **Step 1: Write both scripts** and run them once each to check they work.

- [ ] **Step 2: Run the validation and fill in the report.** For each criterion, write the command, the measured value and pass/fail:
  - SC-001: each of the six scripts with `--runner ray`, 10 times, compared with the local runner's per-host outcome.
  - SC-002: count of final lines whose node matches the host's site.
  - SC-003: `count_sessions.py` before and after two runs per mode.
  - SC-004 and SC-005: `benchmark_ray.py` on the lab.
  - SC-006: `benchmark_ray.py --simulated --hosts-count` 50, 100, 200, 500 (stop early and record the failure if memory runs out).
  - P4: `docker compose -f lab/ray/compose.yml stop ray-site-b`, then `deadlines_sync.py --runner ray --hosts ceos3` and the async one.
  - The "Findings for the design" section of the spec, confirmed or corrected with what the measurements showed.

- [ ] **Step 3: Commit**

```bash
git add scripts/runner/count_sessions.py scripts/runner/benchmark_ray.py scripts/runner/stream_progress_*.py docs/superpowers/reports
git commit -m "docs: record the Ray runner validation results"
```
