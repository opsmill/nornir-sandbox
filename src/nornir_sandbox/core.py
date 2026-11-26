"""Single entry point for creating a Nornir object pointed at the lab."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any, Literal

from nornir import AInitNornir, InitNornir
from nornir.core import Nornir
from nornir.core.task import AggregatedResult

from nornir_sandbox.operations.contracts import ASYNC_CONNECTION, SYNC_CONNECTION
from nornir_sandbox.paths import INVENTORY_DIR, LOG_FILE

RunnerName = Literal["asyncio", "threaded", "serial"]

# Seconds one host may spend on one run before Nornir stops it with TaskTimeoutError.
PER_HOST_TIMEOUT = 60.0

# The asyncio runner runs the `async def` tasks of an Operation, the other runners the
# `def` tasks; each form is registered under its own connection name.
OPERATION_CONNECTION: dict[RunnerName, str] = {
    "asyncio": ASYNC_CONNECTION,
    "threaded": SYNC_CONNECTION,
    "serial": SYNC_CONNECTION,
}


def _settings(
    inventory_dir: Path,
    runner: RunnerName,
    num_workers: int,
    per_host_timeout: float | None,
    timeout: float | None,
) -> dict[str, Any]:
    """Arguments shared by InitNornir and AInitNornir."""
    options: dict[str, Any] = {"per_host_timeout": per_host_timeout, "timeout": timeout}
    if runner != "serial":
        options["num_workers"] = num_workers
    return {
        "runner": {"plugin": runner, "options": options},
        "inventory": {
            "plugin": "SimpleInventory",
            "options": {
                "host_file": str(inventory_dir / "hosts.yaml"),
                "group_file": str(inventory_dir / "groups.yaml"),
                "defaults_file": str(inventory_dir / "defaults.yaml"),
            },
        },
        "logging": {"log_file": str(LOG_FILE)},
        "core": {"operation_connection": OPERATION_CONNECTION[runner]},
    }


def _select(nr: Nornir, hosts: Sequence[str] | None, platform: str | None) -> Nornir:
    if hosts:
        unknown = sorted(set(hosts) - set(nr.inventory.hosts))
        if unknown:
            known = ", ".join(sorted(nr.inventory.hosts))
            raise ValueError(f"unknown host(s): {', '.join(unknown)}; inventory hosts are: {known}")
        wanted = set(hosts)
        nr = nr.filter(filter_func=lambda host: host.name in wanted)
    if platform:
        other = sorted(name for name, host in nr.inventory.hosts.items() if host.platform != platform)
        if hosts and other:
            raise ValueError(
                f"host(s) {', '.join(other)} are not {platform} hosts; this script supports only {platform}"
            )
        nr = nr.filter(platform=platform)
    return nr


async def init_nornir(
    hosts: Sequence[str] | None = None,
    inventory_dir: Path = INVENTORY_DIR,
    num_workers: int = 10,
    per_host_timeout: float | None = PER_HOST_TIMEOUT,
    timeout: float | None = None,
    platform: str | None = None,
) -> Nornir:
    """Return a Nornir object for the lab, optionally limited to some host names or to
    one platform.

    Call it from a coroutine: `nr = await init_nornir(...)`. It uses `AInitNornir`,
    which loads the inventory in a worker thread, so the event loop is not blocked.

    The object uses Nornir's asyncio runner: run coroutine tasks with
    `await nr.arun(...)`, `await nr.asubmit(...)` or `nr.astream(...)`, and close
    the connections with `async with nr:`.
    `num_workers` is the number of hosts that run a task at the same time.
    `per_host_timeout` is the number of seconds one host may spend on one run;
    a host that takes longer gets a failed result with `TaskTimeoutError`, and
    the other hosts are not affected. `timeout` is the number of seconds the
    whole run may take. None removes a limit.

    `core.operation_connection` is set for the runner, so `arun_operation()`
    selects the `async def` task of each Operation without a `connection=` argument.

    `platform` keeps only the hosts of that platform, for scripts that send commands
    of one network operating system; see `nornir_sandbox.eos.PLATFORM`.

    The inventory is read from hosts.yaml, groups.yaml and defaults.yaml in
    `inventory_dir`. Raises ValueError when a requested host is not in the
    inventory, or not of `platform`, so a typo does not silently run against nothing.
    """
    nr = await AInitNornir(**_settings(inventory_dir, "asyncio", num_workers, per_host_timeout, timeout))
    return _select(nr, hosts, platform)


def init_nornir_sync(
    hosts: Sequence[str] | None = None,
    inventory_dir: Path = INVENTORY_DIR,
    num_workers: int = 10,
    runner: RunnerName = "asyncio",
    per_host_timeout: float | None = PER_HOST_TIMEOUT,
    timeout: float | None = None,
    platform: str | None = None,
) -> Nornir:
    """Same as `init_nornir()`, for code that does not run on an event loop.

    It uses `InitNornir`, which blocks while the inventory loads. Use it to read
    the inventory, for example to validate command-line options, or to run plain
    function tasks with another `runner`:

    - "threaded": runs the hosts in threads, with `nr.run(...)`, `nr.submit(...)`
      or `nr.stream(...)`. Close the connections with `with nr:`.
    - "serial": runs one host at a time on the caller's thread, with
      `nr.run(...)` or `nr.stream(...)`. It ignores `num_workers`.

    These two runners stop a host at the time limit only at a checkpoint of the
    task: a `yield`, `task.checkpoint()` or the start of a subtask. With them,
    `run_operation()` selects the `def` (`_sync`) task of each Operation.
    """
    nr = InitNornir(**_settings(inventory_dir, runner, num_workers, per_host_timeout, timeout))
    return _select(nr, hosts, platform)


async def close_failed_connections(nr: Nornir, result: AggregatedResult) -> None:
    """Close the connections of the hosts that failed in `result`.

    Nornir does not close the connection of a host that it stopped at the time
    limit or cancelled, and that connection can still hold the output of the interrupted
    command. Call this before you run another task on the same hosts, so that
    they open a new connection.
    """
    for name, multi in result.items():
        if multi.failed:
            await nr.inventory.hosts[name].aclose_connections()
