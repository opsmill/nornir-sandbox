"""RayRunner with sync tasks, against a local Ray and a simulated inventory."""

from __future__ import annotations

import os
import sys
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import ray
from nornir.core import Nornir
from nornir.core.exceptions import TaskCancelledError, TaskTimeoutError
from nornir.core.inventory import Host
from nornir.core.task import AggregatedResult, MultiResult, PartialResult, Result, Task
from nornir.plugins.runners import ThreadedRunner
from ray import cloudpickle

from nornir_ray import HostActorDiedError, RayClusterUnreachableError, RayRunner, TaskNotSerializableError
from nornir_ray.cluster import check_reachable
from nornir_ray.placement import BalancePolicy, Move, Plan
from nornir_sandbox import init_nornir_sync

cloudpickle.register_pickle_by_value(sys.modules[__name__])

SITES: dict[str, str | None] = {"h1": "site_a", "h2": "site_a", "h3": "site_b"}


def _inventory(tmp_path: Path, sites: dict[str, str | None]) -> Path:
    lines = []
    for name, site in sites.items():
        lines.append(f"{name}:\n  hostname: 192.0.2.1\n")
        if site:
            lines.append(f"  data:\n    site: {site}\n")
    (tmp_path / "hosts.yaml").write_text("---\n" + "".join(lines))
    (tmp_path / "groups.yaml").write_text("---\n{}\n")
    (tmp_path / "defaults.yaml").write_text("---\n{}\n")
    return tmp_path


def _nornir(tmp_path: Path, sites: dict[str, str | None] = SITES, **runner_options: Any) -> tuple[Nornir, RayRunner]:
    runner = RayRunner(**runner_options)
    nr = init_nornir_sync(inventory_dir=_inventory(tmp_path, sites), runner="threaded").with_runner(runner)
    return nr, runner


class Started:
    """Processor that records the hosts whose task started."""

    def __init__(self) -> None:
        self.hosts: list[str] = []

    def task_instance_started(self, task: Task, host: Host) -> None:
        self.hosts.append(host.name)

    def task_started(self, task: Task) -> None:
        pass

    def task_completed(self, task: Task, result: AggregatedResult) -> None:
        pass

    def task_instance_completed(self, task: Task, host: Host, result: MultiResult) -> None:
        pass

    def subtask_instance_started(self, task: Task, host: Host) -> None:
        pass

    def subtask_instance_completed(self, task: Task, host: Host, result: MultiResult) -> None:
        pass


def host_name(task: Task) -> str:
    return task.host.name


def three_steps(task: Task) -> Iterator[PartialResult]:
    for step in range(3):
        yield PartialResult(task.host, f"step {step}", progress=(step + 1) / 3)
    return Result(task.host, result="done")  # type: ignore[return-value]


def count_calls(task: Task) -> int:
    task.host.data["calls"] = task.host.data.get("calls", 0) + 1
    return int(task.host.data["calls"])


def fail(task: Task) -> None:
    raise ValueError("boom")


def tick_slowly(task: Task) -> Iterator[PartialResult]:
    for i in range(100):
        time.sleep(0.1)
        yield PartialResult(task.host, i)


def block(task: Task, seconds: float) -> str:
    time.sleep(seconds)
    return "woke up"


def die(task: Task) -> None:
    os._exit(1)


class FakeConnection:
    def close(self) -> None:
        pass


def open_fake_connection(task: Task) -> None:
    task.host.connections["fake"] = FakeConnection()  # type: ignore[assignment]


def _events(nr: Nornir, task: Any) -> dict[str, list[tuple[str, Any, Any]]]:
    events: dict[str, list[tuple[str, Any, Any]]] = {}
    stream = nr.stream(task=task)
    for event in stream:
        assert event.host is not None
        events.setdefault(event.host.name, []).append(
            (type(event).__name__, event.result, getattr(event, "progress", None))
        )
    stream.result()
    return events


def test_run_returns_results_for_every_host(ray_local: None, tmp_path: Path) -> None:
    nr, _ = _nornir(tmp_path)
    result = nr.run(task=host_name)
    assert {name: multi[0].result for name, multi in result.items()} == {"h1": "h1", "h2": "h2", "h3": "h3"}
    assert not result.failed


def test_stream_events_match_threaded_runner(ray_local: None, tmp_path: Path) -> None:
    nr, _ = _nornir(tmp_path)
    assert _events(nr, three_steps) == _events(nr.with_runner(ThreadedRunner()), three_steps)


def test_hosts_run_on_their_site(ray_local: None, tmp_path: Path) -> None:
    nr, _ = _nornir(tmp_path)
    result = nr.run(task=host_name)
    assert {name: getattr(multi[0], "ray_site", None) for name, multi in result.items()} == SITES
    assert all(getattr(multi[0], "ray_node", None) for multi in result.values())


def test_host_without_site_runs_anywhere(ray_local: None, tmp_path: Path) -> None:
    nr, _ = _nornir(tmp_path, {"h9": None})
    top = nr.run(task=host_name)["h9"][0]
    assert not top.failed
    assert getattr(top, "ray_site", "missing") is None


def test_unserializable_task_fails_before_any_host_starts(ray_local: None, tmp_path: Path) -> None:
    import threading

    lock = threading.Lock()

    def uses_lock(task: Task) -> None:
        with lock:
            pass

    nr, _ = _nornir(tmp_path)
    started = Started()
    with pytest.raises(TaskNotSerializableError, match="uses_lock"):
        nr.with_processors([started]).run(task=uses_lock)
    assert started.hosts == []


def test_unserializable_params_fail_before_any_host_starts(ray_local: None, tmp_path: Path) -> None:
    import threading

    def takes_param(task: Task, lock: Any) -> None:
        pass

    nr, _ = _nornir(tmp_path)
    started = Started()
    with pytest.raises(TaskNotSerializableError, match="takes_param"):
        nr.with_processors([started]).run(task=takes_param, lock=threading.Lock())
    assert started.hosts == []


def test_unreachable_address_fails_with_the_address() -> None:
    started = time.monotonic()
    with pytest.raises(RayClusterUnreachableError, match=r"127\.0\.0\.1:1"):
        check_reachable("127.0.0.1:1", timeout=1)
    assert time.monotonic() - started < 5


def test_task_exception_shows_worker_traceback(ray_local: None, tmp_path: Path) -> None:
    nr, _ = _nornir(tmp_path)
    top = nr.run(task=fail)["h1"][0]
    assert top.failed
    assert isinstance(top.exception, ValueError)
    assert "Traceback" in top.result
    assert "boom" in top.result


def test_actor_reused_across_runs(ray_local: None, tmp_path: Path) -> None:
    nr, runner = _nornir(tmp_path, {"h1": "site_a"})
    assert nr.run(task=count_calls)["h1"][0].result == 1
    first_actor = runner.pool.actor_id("h1")
    assert nr.run(task=count_calls)["h1"][0].result == 2
    assert runner.pool.actor_id("h1") == first_actor


def test_cancel_keeps_actor(ray_local: None, tmp_path: Path) -> None:
    nr, runner = _nornir(tmp_path, {"h1": "site_a"})
    nr.run(task=host_name)
    actor = runner.pool.actor_id("h1")
    handle = nr.submit(task=tick_slowly)
    time.sleep(0.5)
    handle.cancel()
    result = handle.result(timeout=5)
    assert isinstance(result["h1"][0].exception, TaskCancelledError)
    assert runner.pool.actor_id("h1") == actor


def test_deadline_in_blocking_call_kills_actor(ray_local: None, tmp_path: Path) -> None:
    nr, runner = _nornir(tmp_path, {"h1": "site_a"}, per_host_timeout=1)
    nr.run(task=host_name)
    actor = runner.pool.actor_id("h1")
    started = time.monotonic()
    result = nr.run(task=block, seconds=5)
    assert time.monotonic() - started < 2.5
    assert isinstance(result["h1"][0].exception, TaskTimeoutError)
    assert nr.run(task=host_name, on_failed=True)["h1"][0].result == "h1"
    assert runner.pool.actor_id("h1") not in (None, actor)


def test_missing_site_reports_progress_then_times_out(ray_local: None, tmp_path: Path) -> None:
    nr, _ = _nornir(tmp_path, {"h4": "site_c"}, per_host_timeout=2)
    stream = nr.stream(task=host_name)
    events = list(stream)
    assert any(isinstance(e, PartialResult) and "site_c" in str(e.result) for e in events)
    assert isinstance(events[-1].exception, TaskTimeoutError)  # type: ignore[union-attr]
    stream.result()


def test_actor_death_fails_host_without_retry(ray_local: None, tmp_path: Path) -> None:
    nr, runner = _nornir(tmp_path, {"h1": "site_a"})
    top = nr.run(task=die)["h1"][0]
    assert top.failed
    assert isinstance(top.exception, HostActorDiedError)
    assert "h1" in str(top.exception)
    assert "site_a" in str(top.exception)
    assert runner.pool.actor_id("h1") is None


def test_context_exit_closes_worker_connections(ray_local: None, tmp_path: Path) -> None:
    nr, runner = _nornir(tmp_path, {"h1": "site_a"})
    with nr:
        nr.run(task=open_fake_connection)
        actor = runner.pool.get(nr.inventory.hosts["h1"], "site_a")
        assert ray.get(actor.open_connections.remote()) == {"fake": "sync"}
    assert ray.get(actor.open_connections.remote()) == {}


def test_balanced_run_places_every_host_on_a_live_node(ray_local: None, tmp_path: Path) -> None:
    nr, runner = _nornir(tmp_path, balance=BalancePolicy())
    result = nr.run(task=host_name)
    assert not result.failed
    assert runner.last_plan is not None
    assert set(runner.last_plan.assignment) == set(SITES)
    assert set(runner.last_plan.assignment.values()) == {ray.get_runtime_context().get_node_id()}
    assert {move.reason for move in runner.last_plan.moves} == {"new"}


def test_balanced_second_run_moves_nothing_and_keeps_the_actors(ray_local: None, tmp_path: Path) -> None:
    nr, runner = _nornir(tmp_path, balance=BalancePolicy())
    nr.run(task=host_name)
    actors = {name: runner.pool.actor_id(name) for name in SITES}
    nr.run(task=host_name)
    assert runner.last_plan is not None
    assert runner.last_plan.moves == []
    assert {name: runner.pool.actor_id(name) for name in SITES} == actors


def test_balanced_move_replaces_only_the_actor_of_the_moved_host(
    ray_local: None, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    nr, runner = _nornir(tmp_path, balance=BalancePolicy())
    assert nr.run(task=count_calls)["h1"][0].result == 1
    actors = {name: runner.pool.actor_id(name) for name in SITES}
    node = ray.get_runtime_context().get_node_id()

    def move_h1(*args: Any) -> Plan:
        return Plan(dict.fromkeys(SITES, node), [Move("h1", node, node, "rebalance")])

    monkeypatch.setattr("nornir_ray.runner.plan", move_h1)
    result = nr.run(task=count_calls)

    assert result["h1"][0].result == 1
    assert result["h2"][0].result == 2
    assert runner.pool.actor_id("h1") != actors["h1"]
    assert runner.pool.actor_id("h2") == actors["h2"]
