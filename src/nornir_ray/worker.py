"""The Ray actor that runs one host's tasks on a worker.

One actor exists per host. It keeps the `Host` object across calls, so the connections
the tasks open stay open for the next call (one per mode: sync and asyncio). Each call
runs the task through a local `RunContext`, so the task's checkpoints, `PartialResult`
reporting and cancellation behave as they do on the driver's runners.
"""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import AsyncIterator, Callable
from typing import Any

import ray
from nornir.core import Nornir
from nornir.core.configuration import Config
from nornir.core.inventory import Groups, Host, Hosts, Inventory
from nornir.core.processor import Processors
from nornir.core.run import RunContext
from nornir.core.task import AggregatedResult, MultiResult, PartialResult, Task

from nornir_ray.wire import TaskCall, WireEvent, WireFinal, WirePartial, to_wire_partial, to_wire_result


def _is_async(fn: Callable[..., Any]) -> bool:
    return inspect.iscoroutinefunction(fn) or inspect.isasyncgenfunction(fn)


class _ProgressCollector:
    """Processor that hands each partial result of the top-level task to `forward`."""

    def __init__(self, forward: Callable[[PartialResult], None]) -> None:
        self.forward = forward

    def task_instance_progress(self, task: Task, host: Host, partial: PartialResult) -> None:
        if task.parent_task is None:
            self.forward(partial)

    def task_started(self, task: Task) -> None:
        pass

    def task_completed(self, task: Task, result: AggregatedResult) -> None:
        pass

    def task_instance_started(self, task: Task, host: Host) -> None:
        pass

    def task_instance_completed(self, task: Task, host: Host, result: MultiResult) -> None:
        pass

    def subtask_instance_started(self, task: Task, host: Host) -> None:
        pass

    def subtask_instance_completed(self, task: Task, host: Host, result: MultiResult) -> None:
        pass


class HostActorImpl:
    """Runs the tasks of one host, one call at a time."""

    def __init__(self, host: Host, site: str | None) -> None:
        self.host = host
        self.site = site
        self._lock = asyncio.Lock()
        self._run: RunContext | None = None
        self._job: asyncio.Future[MultiResult] | None = None
        self._job_is_async = False
        self._node: str | None = None

    def _nornir_for(self, config: Config) -> Nornir:
        """Return a Nornir object over this host only. The host, and its connections, persist."""
        groups = Groups({group.name: group for group in self.host.groups})
        inventory = Inventory(hosts=Hosts({self.host.name: self.host}), groups=groups, defaults=self.host.defaults)
        return Nornir(inventory=inventory, config=config)

    async def run(self, call: TaskCall) -> AsyncIterator[WireEvent]:
        """Run `call` for the host. Yields each partial result, then the final results."""
        async with self._lock:
            loop = asyncio.get_running_loop()
            partials: asyncio.Queue[WirePartial] = asyncio.Queue()

            def forward(partial: PartialResult) -> None:
                loop.call_soon_threadsafe(partials.put_nowait, to_wire_partial(partial))

            task = Task(
                call.fn,
                self._nornir_for(call.config),
                call.global_dry_run,
                Processors([_ProgressCollector(forward)]),
                call.name,
                call.severity_level,
                **call.params,
            )
            run = RunContext(task, [self.host])
            self._run = run
            self._job_is_async = _is_async(call.fn)
            job: asyncio.Future[MultiResult]
            if self._job_is_async:
                job = asyncio.ensure_future(run.astart_host(task, self.host))
            else:
                job = asyncio.ensure_future(asyncio.to_thread(run.start_host, task, self.host))
            self._job = job
            try:
                while not job.done():
                    getter = asyncio.ensure_future(partials.get())
                    await asyncio.wait({getter, job}, return_when=asyncio.FIRST_COMPLETED)
                    if getter.done():
                        yield getter.result()
                    else:
                        getter.cancel()
                while not partials.empty():
                    yield partials.get_nowait()
                results = job.result()
            finally:
                self._run = None
                self._job = None
            yield WireFinal(results=[to_wire_result(r) for r in results], node=self.node_name(), site=self.site)

    async def cancel(self) -> None:
        """Cancel the current call: a sync task stops at its next checkpoint, an async one at once."""
        run, job = self._run, self._job
        if run is None or job is None:
            return
        run.cancel()
        if self._job_is_async:
            job.cancel()

    def node_name(self) -> str:
        """Return the name of the Ray node this actor runs on, or its id if it has no name."""
        if self._node is None:
            node_id = ray.get_runtime_context().get_node_id()
            names = [node.get("NodeName") for node in ray.nodes() if node.get("NodeID") == node_id]
            self._node = str(names[0]) if names and names[0] else node_id
        return self._node

    async def open_connections(self) -> dict[str, str]:
        """Return each open connection of the host and the mode it was opened in.

        Nornir binds a connection to the mode (`sync` or `asyncio`) that opened it until
        it is closed. A legacy connection plugin has no capability state and is `sync`.
        """
        states = self.host._capability_connections
        names = set(self.host.connections) | set(self.host._async_connections)
        return {name: str(states[name].mode) if name in states else "sync" for name in sorted(names)}


HostActor = ray.remote(num_cpus=0, max_restarts=0, max_task_retries=0)(HostActorImpl)
