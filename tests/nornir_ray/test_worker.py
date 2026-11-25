"""The per-host Ray actor, driven directly with simulated tasks."""

from __future__ import annotations

import asyncio
import sys
import time
from collections.abc import AsyncIterator, Iterator
from typing import Any

import ray
from nornir.core.exceptions import TaskCancelledError
from nornir.core.task import PartialResult, Result, Task
from ray import cloudpickle

from nornir_ray.wire import TaskCall, WireEvent, WireFinal, WirePartial, detach_host
from nornir_ray.worker import HostActor
from nornir_sandbox import init_nornir_sync

# The workers cannot import this test module: send its task functions by value.
cloudpickle.register_pickle_by_value(sys.modules[__name__])


def _actor(site: str | None = None) -> Any:
    nr = init_nornir_sync(runner="threaded")
    return HostActor.remote(detach_host(nr.inventory.hosts["ceos1"]), site)


def _call(fn: Any, **params: Any) -> TaskCall:
    nr = init_nornir_sync(runner="threaded")
    return TaskCall(fn=fn, params=params, name=fn.__name__, severity_level=20, global_dry_run=False, config=nr.config)


def _events(actor: Any, call: TaskCall) -> list[WireEvent]:
    return [ray.get(ref) for ref in actor.run.remote(call)]


def _final(events: list[WireEvent]) -> WireFinal:
    final = events[-1]
    assert isinstance(final, WireFinal)
    return final


def report_then_finish(task: Task) -> Iterator[PartialResult]:
    yield PartialResult(task.host, "a", progress=0.5)
    return Result(task.host, result="done")  # type: ignore[return-value]


async def areport_then_finish(task: Task) -> AsyncIterator[PartialResult | Result]:
    yield PartialResult(task.host, "a", progress=0.5)
    yield Result(task.host, result="done")


def count_calls(task: Task) -> int:
    task.host.data["calls"] = task.host.data.get("calls", 0) + 1
    return int(task.host.data["calls"])


def fail(task: Task) -> None:
    raise ValueError("boom")


def tick_for_ten_seconds(task: Task) -> Iterator[PartialResult]:
    for i in range(50):
        time.sleep(0.2)
        yield PartialResult(task.host, i)


async def sleep_long(task: Task) -> str:
    await asyncio.sleep(30)
    return "slept"


def record_interval(task: Task) -> None:
    started = time.monotonic()
    time.sleep(0.5)
    task.host.data.setdefault("intervals", []).append((started, time.monotonic()))


def read_intervals(task: Task) -> list[tuple[float, float]]:
    return list(task.host.data.get("intervals", []))


def test_sync_generator_task_streams_partials_then_final(ray_local: None) -> None:
    events = _events(_actor(), _call(report_then_finish))
    assert events[:-1] == [WirePartial("a", 0.5)]
    assert _final(events).results[0].result == "done"


def test_async_generator_task_streams_partials_then_final(ray_local: None) -> None:
    events = _events(_actor(), _call(areport_then_finish))
    assert events[:-1] == [WirePartial("a", 0.5)]
    assert _final(events).results[0].result == "done"


def test_host_state_persists_across_calls(ray_local: None) -> None:
    actor = _actor()
    assert _final(_events(actor, _call(count_calls))).results[0].result == 1
    assert _final(_events(actor, _call(count_calls))).results[0].result == 2


def test_task_exception_keeps_worker_traceback(ray_local: None) -> None:
    top = _final(_events(_actor(), _call(fail))).results[0]
    assert top.failed
    assert isinstance(top.exception, ValueError)
    assert "Traceback" in top.result
    assert "boom" in top.result


def test_cancel_stops_sync_task_at_next_checkpoint(ray_local: None) -> None:
    actor = _actor()
    gen = actor.run.remote(_call(tick_for_ten_seconds))
    ray.get(next(gen))
    started = time.monotonic()
    actor.cancel.remote()
    events = [ray.get(ref) for ref in gen]
    assert time.monotonic() - started < 1
    assert isinstance(_final(events).results[0].exception, TaskCancelledError)


def test_cancel_stops_async_task_at_awaited_call(ray_local: None) -> None:
    actor = _actor()
    gen = actor.run.remote(_call(sleep_long))
    time.sleep(0.5)
    started = time.monotonic()
    actor.cancel.remote()
    events = [ray.get(ref) for ref in gen]
    assert time.monotonic() - started < 1
    assert isinstance(_final(events).results[0].exception, TaskCancelledError)


def test_calls_on_one_actor_do_not_overlap(ray_local: None) -> None:
    actor = _actor()
    first = actor.run.remote(_call(record_interval))
    second = actor.run.remote(_call(record_interval))
    for gen in (first, second):
        _final([ray.get(ref) for ref in gen])
    (_, a_end), (b_start, _) = sorted(_final(_events(actor, _call(read_intervals))).results[0].result)
    assert a_end <= b_start


def test_final_names_node_and_site(ray_local: None) -> None:
    final = _final(_events(_actor(site="site_a"), _call(count_calls)))
    assert final.site == "site_a"
    assert isinstance(final.node, str)
    assert final.node


def test_open_connections_is_empty_for_a_new_actor(ray_local: None) -> None:
    assert ray.get(_actor().open_connections.remote()) == {}
