"""pyproject.toml registers every Operation of the sandbox with a sync and an async task
for each lab platform, and init_nornir() and init_nornir_sync() select the connection name of the runner."""

from __future__ import annotations

import asyncio
from typing import assert_type

from nornir.core.operations import OperationRegistry
from nornir.core.run import RunStream

from nornir_sandbox import init_nornir, init_nornir_sync
from nornir_sandbox.operations import ASYNC_CONNECTION, SYNC_CONNECTION, Facts, GetFacts

OPERATIONS = {"get_config", "get_facts", "get_interfaces", "get_lldp_neighbors", "set_interface_descriptions"}
PLATFORMS = {"arista_eos", "nokia_srlinux"}


def test_entry_points_register_a_sync_and_an_async_task_per_platform() -> None:
    infos = {info.name: info for info in OperationRegistry.operations() if info.name in OPERATIONS}

    assert set(infos) == OPERATIONS
    for info in infos.values():
        assert info.operation is not None
        keys = {(r.platform, r.connection, r.kind) for r in info.implementations}
        assert keys == {
            (platform, connection, kind)
            for platform in PLATFORMS
            for connection, kind in ((ASYNC_CONNECTION, "async"), (SYNC_CONNECTION, "sync"))
        }
        assert all(r.source.distribution == "nornir-sandbox" for r in info.implementations)


def test_init_nornir_selects_the_operation_connection_of_the_runner() -> None:
    assert asyncio.run(init_nornir()).config.core.operation_connection == ASYNC_CONNECTION
    assert init_nornir_sync().config.core.operation_connection == ASYNC_CONNECTION
    assert init_nornir_sync(runner="threaded").config.core.operation_connection == SYNC_CONNECTION
    assert init_nornir_sync(runner="serial").config.core.operation_connection == SYNC_CONNECTION


def check_result_types(stream: RunStream[Facts]) -> None:
    """Not run: mypy checks that the Operation class types the results."""
    for res in stream.results():
        assert_type(res.result, Facts)


def check_run_operation_type() -> None:
    """Not run: mypy checks that run_operation(GetFacts) returns a RunStream[Facts]."""
    with init_nornir_sync(runner="serial") as nr:
        assert_type(nr.run_operation(GetFacts), RunStream[Facts])
