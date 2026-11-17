"""Single entry point for creating a Nornir object pointed at the lab."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

from nornir import AInitNornir, InitNornir
from nornir.core import Nornir
from nornir.core.task import AggregatedResult

from nornir_sandbox.paths import INVENTORY_DIR, LOG_FILE

# Seconds one host may spend on one run before Nornir stops it with TaskTimeoutError.
PER_HOST_TIMEOUT = 60.0


def _settings(inventory_dir: Path, num_workers: int, per_host_timeout: float | None) -> dict[str, Any]:
    """Arguments shared by InitNornir and AInitNornir."""
    return {
        "runner": {
            "plugin": "asyncio",
            "options": {"num_workers": num_workers, "per_host_timeout": per_host_timeout},
        },
        "inventory": {
            "plugin": "SimpleInventory",
            "options": {
                "host_file": str(inventory_dir / "hosts.yaml"),
                "group_file": str(inventory_dir / "groups.yaml"),
                "defaults_file": str(inventory_dir / "defaults.yaml"),
            },
        },
        "logging": {"log_file": str(LOG_FILE)},
    }


def _select(nr: Nornir, hosts: Sequence[str] | None) -> Nornir:
    if not hosts:
        return nr
    unknown = sorted(set(hosts) - set(nr.inventory.hosts))
    if unknown:
        known = ", ".join(sorted(nr.inventory.hosts))
        raise ValueError(f"unknown host(s): {', '.join(unknown)}; inventory hosts are: {known}")
    wanted = set(hosts)
    return nr.filter(filter_func=lambda host: host.name in wanted)


async def init_nornir(
    hosts: Sequence[str] | None = None,
    inventory_dir: Path = INVENTORY_DIR,
    num_workers: int = 10,
    per_host_timeout: float | None = PER_HOST_TIMEOUT,
) -> Nornir:
    """Return a Nornir object for the lab, optionally limited to some host names.

    Call it from a coroutine: `nr = await init_nornir(...)`. It uses `AInitNornir`,
    which loads the inventory in a worker thread, so the event loop is not blocked.

    The object uses Nornir's asyncio runner: run coroutine tasks with
    `await nr.arun(...)` and close the connections with `async with nr:`.
    `num_workers` is the number of hosts that run a task at the same time.
    `per_host_timeout` is the number of seconds one host may spend on one run;
    a host that takes longer gets a failed result with `TaskTimeoutError`, and
    the other hosts are not affected. None removes the limit.

    The inventory is read from hosts.yaml, groups.yaml and defaults.yaml in
    `inventory_dir`. Raises ValueError when a requested host is not in the
    inventory, so a typo does not silently run against nothing.
    """
    nr = await AInitNornir(**_settings(inventory_dir, num_workers, per_host_timeout))
    return _select(nr, hosts)


def init_nornir_sync(
    hosts: Sequence[str] | None = None,
    inventory_dir: Path = INVENTORY_DIR,
    num_workers: int = 10,
    per_host_timeout: float | None = PER_HOST_TIMEOUT,
) -> Nornir:
    """Same as `init_nornir()`, for code that does not run on an event loop.

    It uses `InitNornir`, which blocks while the inventory loads. Use it only to
    read the inventory, for example to validate command-line options.
    """
    return _select(InitNornir(**_settings(inventory_dir, num_workers, per_host_timeout)), hosts)


async def close_failed_connections(nr: Nornir, result: AggregatedResult) -> None:
    """Close the connections of the hosts that failed in `result`.

    Nornir does not close the connection of a host that it stopped at the time
    limit, and that connection can still hold the output of the interrupted
    command. Call this before you run another task on the same hosts, so that
    they open a new connection.
    """
    for name, multi in result.items():
        if multi.failed:
            await nr.inventory.hosts[name].aclose_connections()
