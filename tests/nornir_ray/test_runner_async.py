"""RayRunner with async tasks, against a local Ray and a simulated inventory."""

from __future__ import annotations

import asyncio
import sys
import time
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import ray
from nornir.core import Nornir
from nornir.core.exceptions import ConnectionModeMismatchError, TaskCancelledError, TaskTimeoutError
from nornir.core.task import PartialResult, Result, Task
from nornir.plugins.runners import AsyncioRunner
from ray import cloudpickle

from nornir_ray import RayRunner
from nornir_sandbox import init_nornir

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


async def _nornir(
    tmp_path: Path, sites: dict[str, str | None] = SITES, **runner_options: Any
) -> tuple[Nornir, RayRunner]:
    runner = RayRunner(**runner_options)
    nr = await init_nornir(inventory_dir=_inventory(tmp_path, sites))
    return nr.with_runner(runner), runner


async def host_name(task: Task) -> str:
    return task.host.name


async def three_steps(task: Task) -> AsyncIterator[PartialResult | Result]:
    for step in range(3):
        yield PartialResult(task.host, f"step {step}", progress=(step + 1) / 3)
    yield Result(task.host, result="done")


async def sleep_long(task: Task) -> str:
    await asyncio.sleep(30)
    return "slept"


class FakePlugin:
    """Connection plugin that opens in either mode without any device."""

    def __init__(self) -> None:
        self._connection: object | None = None

    def get_capabilities(self) -> frozenset[str]:
        return frozenset({"sync", "asyncio"})

    @property
    def connection(self) -> Any:
        return self._connection

    def open(self, *args: Any, **kwargs: Any) -> None:
        self._connection = object()

    def close(self) -> None:
        self._connection = None

    async def aopen(self, *args: Any, **kwargs: Any) -> None:
        self._connection = object()

    async def aclose(self) -> None:
        self._connection = None


def _register_fake_plugin() -> None:
    # Runs on the worker: the registry is per process. Import it here, so the task does
    # not carry a pickled copy of the driver's registry.
    from nornir.core.plugins.connections import CapabilityConnectionPluginRegister

    if "fake" not in CapabilityConnectionPluginRegister.available:
        CapabilityConnectionPluginRegister.register("fake", FakePlugin)  # type: ignore[arg-type]


def open_sync_connection(task: Task) -> None:
    _register_fake_plugin()
    task.host.get_connection("fake", task.nornir.config)


async def open_async_connection(task: Task) -> None:
    _register_fake_plugin()
    await task.host.aget_connection("fake", task.nornir.config)


async def _events(nr: Nornir, task: Any) -> dict[str, list[tuple[str, Any, Any]]]:
    events: dict[str, list[tuple[str, Any, Any]]] = {}
    stream = nr.astream(task=task)
    async for event in stream:
        assert event.host is not None
        events.setdefault(event.host.name, []).append(
            (type(event).__name__, event.result, getattr(event, "progress", None))
        )
    await stream.result()
    return events


def test_arun_returns_results_for_every_host(ray_local: None, tmp_path: Path) -> None:
    async def main() -> None:
        nr, _ = await _nornir(tmp_path)
        result = await nr.arun(task=host_name)
        assert {name: multi[0].result for name, multi in result.items()} == {"h1": "h1", "h2": "h2", "h3": "h3"}

    asyncio.run(main())


def test_astream_events_match_asyncio_runner(ray_local: None, tmp_path: Path) -> None:
    async def main() -> None:
        nr, _ = await _nornir(tmp_path)
        assert await _events(nr, three_steps) == await _events(nr.with_runner(AsyncioRunner()), three_steps)

    asyncio.run(main())


def test_async_hosts_run_on_their_site(ray_local: None, tmp_path: Path) -> None:
    async def main() -> None:
        nr, _ = await _nornir(tmp_path)
        result = await nr.arun(task=host_name)
        assert {name: getattr(multi[0], "ray_site", None) for name, multi in result.items()} == SITES

    asyncio.run(main())


def test_async_cancel_stops_at_awaited_call_and_keeps_actor(ray_local: None, tmp_path: Path) -> None:
    async def main() -> None:
        nr, runner = await _nornir(tmp_path, {"h1": "site_a"})
        await nr.arun(task=host_name)
        actor = runner.pool.actor_id("h1")
        handle = await nr.asubmit(task=sleep_long)
        await asyncio.sleep(0.5)
        started = time.monotonic()
        handle.cancel()
        result = await handle.result()
        assert time.monotonic() - started < 1
        assert isinstance(result["h1"][0].exception, TaskCancelledError)
        assert runner.pool.actor_id("h1") == actor
        assert (await nr.arun(task=host_name, on_failed=True))["h1"][0].result == "h1"

    asyncio.run(main())


def test_async_deadline_keeps_actor(ray_local: None, tmp_path: Path) -> None:
    async def main() -> None:
        nr, runner = await _nornir(tmp_path, {"h1": "site_a"}, per_host_timeout=1)
        await nr.arun(task=host_name)
        actor = runner.pool.actor_id("h1")
        started = time.monotonic()
        result = await nr.arun(task=sleep_long)
        assert time.monotonic() - started < 2
        assert isinstance(result["h1"][0].exception, TaskTimeoutError)
        assert runner.pool.actor_id("h1") == actor

    asyncio.run(main())


def test_async_missing_site_reports_progress_then_times_out(ray_local: None, tmp_path: Path) -> None:
    async def main() -> None:
        nr, _ = await _nornir(tmp_path, {"h4": "site_c"}, per_host_timeout=2)
        stream = nr.astream(task=host_name)
        events = [event async for event in stream]
        assert any(isinstance(e, PartialResult) and "site_c" in str(e.result) for e in events)
        assert isinstance(events[-1], Result)
        assert isinstance(events[-1].exception, TaskTimeoutError)
        await stream.result()

    asyncio.run(main())


def test_async_context_exit_closes_worker_connections(ray_local: None, tmp_path: Path) -> None:
    async def main() -> Any:
        nr, runner = await _nornir(tmp_path, {"h1": "site_a"})
        async with nr:
            result = await nr.arun(task=open_async_connection)
            assert not result.failed, result["h1"][0].result
            actor = runner.pool.get(nr.inventory.hosts["h1"], "site_a")
            assert await actor.open_connections.remote() == {"fake": "asyncio"}
        return actor

    actor = asyncio.run(main())
    assert ray.get(actor.open_connections.remote()) == {}


def test_connection_stays_in_the_mode_that_opened_it(ray_local: None, tmp_path: Path) -> None:
    """Nornir 3.7.0a1 binds a connection name to one mode: the actor keeps one connection per host."""

    async def main() -> Any:
        nr, runner = await _nornir(tmp_path, {"h1": "site_a"})
        assert not (await asyncio.to_thread(nr.run, task=open_sync_connection)).failed
        actor = runner.pool.get(nr.inventory.hosts["h1"], "site_a")
        assert await actor.open_connections.remote() == {"fake": "sync"}

        mismatch = await nr.arun(task=open_async_connection)
        assert isinstance(mismatch["h1"][0].exception, ConnectionModeMismatchError)

        await nr.aclose_connections(on_good=True, on_failed=True)
        assert not (await nr.arun(task=open_async_connection, on_failed=True)).failed
        return actor

    actor = asyncio.run(main())
    assert ray.get(actor.open_connections.remote()) == {"fake": "asyncio"}
