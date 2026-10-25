"""Arista EOS tasks for the Operations in contracts.py.

Each Operation has an `async def` task for the asyncio runner and a `def` task, named
with `_sync`, for the threaded and serial runners. pyproject.toml registers them as
`<operation>.arista_eos.scrapli` and `<operation>.arista_eos.scrapli_sync`.
"""

from __future__ import annotations

from nornir.core.task import Result, Task

from nornir_sandbox.eos import configure, configure_sync, send, send_json, send_json_sync, send_sync
from nornir_sandbox.operations.contracts import (
    Config,
    Facts,
    GetConfigInput,
    GetFactsInput,
    GetLldpNeighborsInput,
    LldpNeighbors,
    SetInterfaceDescriptionsInput,
    SetInterfaceDescriptionsOutput,
)
from nornir_sandbox.operations.parse import (
    description_changes,
    description_result,
    eos_facts,
    eos_interface_descriptions,
    eos_lldp_neighbors,
)


def _description_lines(changes: dict[str, str]) -> list[str]:
    lines = []
    for interface, description in changes.items():
        lines += [f"interface {interface}", f"description {description}" if description else "no description"]
    return lines


async def get_facts(task: Task, params: GetFactsInput) -> Facts:
    return eos_facts(await send_json(task, "show version"), await send_json(task, "show hostname"))


def get_facts_sync(task: Task, params: GetFactsInput) -> Facts:
    return eos_facts(send_json_sync(task, "show version"), send_json_sync(task, "show hostname"))


async def get_config(task: Task, params: GetConfigInput) -> Config:
    return Config(config=await send(task, "show running-config"))


def get_config_sync(task: Task, params: GetConfigInput) -> Config:
    return Config(config=send_sync(task, "show running-config"))


async def get_lldp_neighbors(task: Task, params: GetLldpNeighborsInput) -> LldpNeighbors:
    return eos_lldp_neighbors(await send_json(task, "show lldp neighbors"))


def get_lldp_neighbors_sync(task: Task, params: GetLldpNeighborsInput) -> LldpNeighbors:
    return eos_lldp_neighbors(send_json_sync(task, "show lldp neighbors"))


async def set_interface_descriptions(
    task: Task, params: SetInterfaceDescriptionsInput
) -> Result[SetInterfaceDescriptionsOutput]:
    current = eos_interface_descriptions(await send_json(task, "show interfaces"))
    changes = description_changes(current, params.descriptions)
    if changes:
        await configure(task, _description_lines(changes))
    return description_result(task, current, params.descriptions)


def set_interface_descriptions_sync(
    task: Task, params: SetInterfaceDescriptionsInput
) -> Result[SetInterfaceDescriptionsOutput]:
    current = eos_interface_descriptions(send_json_sync(task, "show interfaces"))
    changes = description_changes(current, params.descriptions)
    if changes:
        configure_sync(task, _description_lines(changes))
    return description_result(task, current, params.descriptions)
