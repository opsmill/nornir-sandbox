"""Single entry point for creating a Nornir object pointed at the lab."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

from nornir import InitNornir
from nornir.core import Nornir

from nornir_sandbox.paths import INVENTORY_DIR, LOG_FILE


def init_nornir(
    hosts: Sequence[str] | None = None,
    inventory_dir: Path = INVENTORY_DIR,
    num_workers: int = 10,
) -> Nornir:
    """Return a Nornir object for the lab, optionally limited to some host names.

    The object uses Nornir's asyncio runner: run coroutine tasks with
    `await nr.arun(...)` and close the connections with `async with nr:`.
    `num_workers` is the number of hosts that run a task at the same time.

    The inventory is read from hosts.yaml, groups.yaml and defaults.yaml in
    `inventory_dir`. Raises ValueError when a requested host is not in the
    inventory, so a typo does not silently run against nothing.
    """
    nr = InitNornir(
        runner={"plugin": "asyncio", "options": {"num_workers": num_workers}},
        inventory={
            "plugin": "SimpleInventory",
            "options": {
                "host_file": str(inventory_dir / "hosts.yaml"),
                "group_file": str(inventory_dir / "groups.yaml"),
                "defaults_file": str(inventory_dir / "defaults.yaml"),
            },
        },
        logging={"log_file": str(LOG_FILE)},
    )
    if hosts:
        unknown = sorted(set(hosts) - set(nr.inventory.hosts))
        if unknown:
            known = ", ".join(sorted(nr.inventory.hosts))
            raise ValueError(f"unknown host(s): {', '.join(unknown)}; inventory hosts are: {known}")
        wanted = set(hosts)
        nr = nr.filter(filter_func=lambda host: host.name in wanted)
    return nr
