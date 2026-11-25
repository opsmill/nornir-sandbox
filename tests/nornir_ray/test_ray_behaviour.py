"""Ray behaviours the runner relies on, checked on the pinned Ray version.

If one of these fails after a Ray upgrade, the runner's design needs another look before
its own tests are trusted.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from typing import Any

import ray


@ray.remote(num_cpus=0)
class Probe:
    def __init__(self) -> None:
        self.stop = asyncio.Event()

    async def count(self, delay: float = 0.0) -> AsyncIterator[int]:
        await asyncio.sleep(delay)
        for i in (1, 2, 3):
            yield i

    async def wait_until_stopped(self) -> str:
        try:
            await asyncio.wait_for(self.stop.wait(), timeout=30)
        except TimeoutError:
            return "timed out"
        return "stopped"

    async def request_stop(self) -> None:
        self.stop.set()

    async def resources(self) -> dict[str, Any]:
        return dict(ray.get_runtime_context().get_assigned_resources())

    async def ping(self) -> str:
        return "pong"


def test_async_actor_streams_generator_items(ray_local: None) -> None:
    gen = Probe.remote().count.remote()  # type: ignore[attr-defined]
    assert [ray.get(ref) for ref in gen] == [1, 2, 3]


def test_wait_on_streaming_generator_times_out_then_returns(ray_local: None) -> None:
    gen = Probe.remote().count.remote(1.0)  # type: ignore[attr-defined]
    ready, pending = ray.wait([gen], timeout=0.1)
    assert (ready, pending) == ([], [gen])
    ready, pending = ray.wait([gen], timeout=5)
    assert (ready, pending) == ([gen], [])


def test_cancel_reaches_running_async_actor_call(ray_local: None) -> None:
    probe = Probe.remote()  # type: ignore[attr-defined]
    waiting = probe.wait_until_stopped.remote()
    time.sleep(0.3)
    started = time.monotonic()
    probe.request_stop.remote()
    assert ray.get(waiting, timeout=5) == "stopped"
    assert time.monotonic() - started < 2


def test_custom_resource_places_actor(ray_local: None) -> None:
    probe = Probe.options(resources={"site_b": 0.01}).remote()  # type: ignore[attr-defined]
    assert "site_b" in ray.get(probe.resources.remote())


def test_missing_resource_keeps_actor_pending(ray_local: None) -> None:
    probe = Probe.options(resources={"site_c": 0.01}).remote()  # type: ignore[attr-defined]
    ready, _ = ray.wait([probe.ping.remote()], timeout=2)
    assert ready == []
    assert "site_c" not in ray.cluster_resources()
    ray.kill(probe)
