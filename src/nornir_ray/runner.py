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

With `balance`, the runner chooses the node of each host itself (see `placement.py`),
among the nodes of the host's site, so the hosts spread over the nodes present. Before
each run it updates the placement, and replaces the actor of each host that moves.
"""

from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import AsyncGenerator, Generator, Iterator
from typing import Any

import ray
from nornir.core.exceptions import TaskInterruptedError, TaskTimeoutError
from nornir.core.inventory import Host
from nornir.core.plugins.runners import CancellationDepth
from nornir.core.run import RunContext, RunOptions
from nornir.core.task import AggregatedResult, PartialResult, Result, Task
from nornir.plugins.runners import AsyncioRunner, ThreadedRunner

from nornir_ray.cluster import ActorPool, connect, resolve_address, site_available
from nornir_ray.errors import HostActorDiedError
from nornir_ray.placement import BalancePolicy, Node, Plan, plan
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
        balance: Choose the node of each host among the nodes of its site and move hosts
            when nodes join or leave, under this policy. None lets Ray place each host on
            any node of its site, once

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
        balance: BalancePolicy | None = None,
    ) -> None:
        self.address = address
        self.num_workers = num_workers
        self.run_options = RunOptions(per_host_timeout, timeout)
        self.site_key = site_key
        self.resources = resources
        self.pool = ActorPool()
        self.balance = balance
        # The placement of the last run, with `balance`.
        self.last_plan: Plan | None = None
        self._assignment: dict[str, str] = {}
        # When the runner first saw each node; the nodes of the first run need no warm-up.
        self._first_seen: dict[str, float] = {}
        # Actor of each host whose sync proxy is still waiting for it.
        self._in_flight: dict[str, Any] = {}
        self._in_flight_lock = threading.Lock()

    def _prepare(self, task: Task) -> TaskCall:
        """Connect to Ray and check that the task can travel, before any host starts."""
        connect(resolve_address(self.address), self.resources)
        call = _task_call(task)
        ensure_serializable(call)
        return call

    def _live_nodes(self, balance: BalancePolicy) -> list[Node]:
        """Return the live Ray nodes; a node's weight is its number of CPUs."""
        now = time.monotonic()
        first_call = not self._first_seen
        nodes = []
        for info in ray.nodes():
            if not info.get("Alive"):
                continue
            node_id = str(info["NodeID"])
            seen = self._first_seen.setdefault(node_id, float("-inf") if first_call else now)
            resources: dict[str, float] = info.get("Resources", {})
            nodes.append(
                Node(
                    node_id=node_id,
                    name=str(info.get("NodeName", node_id)),
                    sites=frozenset(resources),
                    weight=float(resources.get("CPU", 0.0)),
                    ready=now - seen >= balance.warm_up,
                )
            )
        return nodes

    def _place(self, hosts: list[Host]) -> None:
        """Update the placement of `hosts`, and stop the actor of each host that moves.

        A host whose actor is still busy from another run keeps it until a later run.
        """
        if self.balance is None:
            return
        result = plan(
            {h.name: self._site(h) for h in hosts}, self._live_nodes(self.balance), self.pool.nodes(), self.balance
        )
        with self._in_flight_lock:
            busy = set(self._in_flight)
        for move in result.moves:
            if move.source is not None and move.host not in busy:
                self.pool.kill(move.host)
        self._assignment = result.assignment
        self.last_plan = result

    def _actor(self, host: Host, site: str | None) -> Any:
        return self.pool.get(host, site, self._assignment.get(host.name))

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
        self._place(hosts)
        proxy = self._sync_proxy(call, run)
        proxy_task = Task(proxy, task.nornir, task.global_dry_run, task.processors, task.name, task.severity_level)
        run.add_cancel_callback(lambda: self._cancel_running(run))
        yield from ThreadedRunner(self.num_workers).execute(proxy_task, hosts, run)
        self._kill_timed_out(run)

    async def arun(self, task: Task, hosts: list[Host]) -> AggregatedResult:
        """Run the asynchronous task against the hosts and return their results."""
        run = RunContext(task, hosts, self.run_options)
        await self.aexecute(task, hosts, run)
        return run.aggregated()

    async def aexecute(self, task: Task, hosts: list[Host], run: RunContext) -> None:
        """Run the hosts through async proxy tasks on Nornir's asyncio runner."""
        await asyncio.to_thread(connect, resolve_address(self.address), self.resources)
        call = _task_call(task)
        ensure_serializable(call)
        await asyncio.to_thread(self._place, hosts)
        proxy = self._async_proxy(call)
        proxy_task = Task(proxy, task.nornir, task.global_dry_run, task.processors, task.name, task.severity_level)
        await AsyncioRunner(self.num_workers).aexecute(proxy_task, hosts, run)

    def _async_proxy(self, call: TaskCall) -> Any:
        runner = self

        async def aproxy(task: Task) -> AsyncGenerator[PartialResult | Result, None]:
            host = task.host
            site = runner._site(host)
            if site and not site_available(site):
                yield PartialResult(host, f"no Ray worker for site {site}; waiting")
            actor = runner._actor(host, site)
            try:
                async for ref in actor.run.remote(call):
                    event = await ref
                    if isinstance(event, WirePartial):
                        yield from_wire_partial(event, host)
                    elif isinstance(event, WireFinal):
                        yield _final_result(task, event)
                        return
            except (asyncio.CancelledError, TaskInterruptedError):
                # Cancelled or past its deadline: stop the call on the actor, keep the actor.
                actor.cancel.remote()
                raise
            except ray.exceptions.RayActorError as error:
                runner.pool.forget(host.name, actor)
                raise HostActorDiedError(host.name, None, site, str(error)) from error

        return aproxy

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
            actor = runner._actor(host, site)
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
