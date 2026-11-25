"""`RayRunner`: a Nornir runner that runs each host's task on a Ray actor placed by site.

The runner does not record results itself. It wraps the task in a proxy task and hands
it to Nornir's own runner (`ThreadedRunner` for sync runs, `AsyncioRunner` for async
runs), so Nornir core records results, partial results, cancellation and deadlines
through its public API. Each proxy sends the real task to the host's actor and relays
the actor's events back.

Cancellation keeps the actor and its connection: a sync task stops at its next
checkpoint, an async task at the call it is awaiting. A sync host that passes its
deadline has its actor killed, because the call may be blocked; the next run creates a
new actor and a new connection.
"""

from __future__ import annotations

import threading
from collections.abc import Generator, Iterator
from typing import Any

import ray
from nornir.core.exceptions import TaskInterruptedError, TaskTimeoutError
from nornir.core.inventory import Host
from nornir.core.plugins.runners import CancellationDepth
from nornir.core.run import RunContext, RunOptions
from nornir.core.task import AggregatedResult, PartialResult, Result, Task
from nornir.plugins.runners import ThreadedRunner

from nornir_ray.cluster import ActorPool, connect, resolve_address, site_available
from nornir_ray.errors import HostActorDiedError
from nornir_ray.wire import (
    TaskCall,
    WireFinal,
    WirePartial,
    ensure_serializable,
    from_wire_partial,
    from_wire_result,
)

# Seconds a sync proxy waits for the next event before it checks its host's deadline.
POLL_INTERVAL = 0.1


def _task_call(task: Task) -> TaskCall:
    return TaskCall(
        fn=task.task,
        params=dict(task.params),
        name=task.name,
        severity_level=task.severity_level,
        global_dry_run=task.global_dry_run,
        config=task.nornir.config,
    )


def _final_result(task: Task, final: WireFinal) -> Result:
    """Return the host's result from `final`, after adding its subtask results to `task`."""
    host = task.host
    task.results.extend(from_wire_result(wire, host) for wire in final.results[1:])
    return from_wire_result(final.results[0], host, ray_node=final.node, ray_site=final.site)


class RayRunner:
    """Runner that executes the task of each host on a Ray actor placed by site.

    Arguments:
        address: Address of the Ray cluster (`host:port`). None uses the `RAY_ADDRESS`
            environment variable, or starts a local Ray if it is not set
        num_workers: Maximum number of hosts in flight at once on the driver
        per_host_timeout: Seconds each host may run, counted from the moment it starts
        timeout: Seconds the whole run may take
        site_key: Host attribute whose value names the Ray resource of the host's site
        resources: Custom resources of a local Ray, for example `{"site_a": 100}`;
            ignored when connecting to a cluster

    """

    cancellation_depth: CancellationDepth = "checkpoint"
    supports_background = True
    run_options = RunOptions()

    def __init__(
        self,
        address: str | None = None,
        num_workers: int = 20,
        per_host_timeout: float | None = None,
        timeout: float | None = None,
        site_key: str = "site",
        resources: dict[str, float] | None = None,
    ) -> None:
        self.address = address
        self.num_workers = num_workers
        self.run_options = RunOptions(per_host_timeout, timeout)
        self.site_key = site_key
        self.resources = resources
        self.pool = ActorPool()
        # Actor of each host whose sync proxy is still waiting for it.
        self._in_flight: dict[str, Any] = {}
        self._in_flight_lock = threading.Lock()

    def _prepare(self, task: Task) -> TaskCall:
        """Connect to Ray and check that the task can travel, before any host starts."""
        connect(resolve_address(self.address), self.resources)
        call = _task_call(task)
        ensure_serializable(call)
        return call

    def _site(self, host: Host) -> str | None:
        site = host.get(self.site_key)
        return str(site) if site else None

    def run(self, task: Task, hosts: list[Host]) -> AggregatedResult:
        """Run the task against the hosts and return their results."""
        run = RunContext(task, hosts, self.run_options)
        for _ in self.execute(task, hosts, run):
            pass
        return run.aggregated()

    def execute(self, task: Task, hosts: list[Host], run: RunContext) -> Iterator[None]:
        """Run the hosts through proxy tasks on Nornir's threaded runner."""
        call = self._prepare(task)
        proxy = self._sync_proxy(call, run)
        proxy_task = Task(proxy, task.nornir, task.global_dry_run, task.processors, task.name, task.severity_level)
        run.add_cancel_callback(lambda: self._cancel_running(run))
        yield from ThreadedRunner(self.num_workers).execute(proxy_task, hosts, run)
        self._kill_timed_out(run)

    def _kill_timed_out(self, run: RunContext) -> None:
        """Kill the actors of timed-out hosts whose call is still running.

        The run can end before a blocked proxy notices its host's deadline; killing
        here, before the run returns, keeps the next run from reusing that actor.
        """
        timed_out = {r.host.name for r in run.completed() if isinstance(r.exception, TaskTimeoutError) and r.host}
        with self._in_flight_lock:
            stuck = {name: actor for name, actor in self._in_flight.items() if name in timed_out}
        for name, actor in stuck.items():
            self.pool.kill(name, actor)

    def _cancel_running(self, run: RunContext) -> None:
        for host in run.running_hosts():
            self.pool.cancel(host.name)

    def _sync_proxy(self, call: TaskCall, run: RunContext) -> Any:
        runner = self

        def proxy(task: Task) -> Generator[PartialResult, None, Result]:
            host = task.host
            site = runner._site(host)
            if site and not site_available(site):
                yield PartialResult(host, f"no Ray worker for site {site}; waiting")
            actor = runner.pool.get(host, site)
            events = actor.run.remote(call)
            with runner._in_flight_lock:
                runner._in_flight[host.name] = actor
            try:
                return (yield from runner._relay(task, actor, events, site, run))
            finally:
                with runner._in_flight_lock:
                    if runner._in_flight.get(host.name) is actor:
                        del runner._in_flight[host.name]

        return proxy

    def _relay(
        self, task: Task, actor: Any, events: Any, site: str | None, run: RunContext
    ) -> Generator[PartialResult, None, Result]:
        """Relay the actor's events for `task` until its final result."""
        host = task.host
        while True:
            try:
                ready, _ = ray.wait([events], timeout=POLL_INTERVAL)
                if not ready:
                    if host.name not in {h.name for h in run.running_hosts()}:
                        # Recorded by a deadline while the call may be blocked.
                        self.pool.kill(host.name, actor)
                        return Result(host, failed=True)
                    continue
                event = ray.get(next(events))
            except ray.exceptions.RayActorError as error:
                self.pool.forget(host.name, actor)
                raise HostActorDiedError(host.name, None, site, str(error)) from error
            if isinstance(event, WirePartial):
                try:
                    yield from_wire_partial(event, host)
                except TaskInterruptedError:
                    actor.cancel.remote()
                    raise
            elif isinstance(event, WireFinal):
                return _final_result(task, event)
