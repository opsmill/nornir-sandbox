"""Nokia SR Linux tasks for the Operations in contracts.py.

Each Operation has an `async def` task for the asyncio runner and a `def` task, named
with `_sync`, for the threaded and serial runners. pyproject.toml registers them as
`<operation>.nokia_srlinux.scrapli` and `<operation>.nokia_srlinux.scrapli_sync`.
"""

from __future__ import annotations

from nornir.core.task import Result, Task

from nornir_sandbox.operations.contracts import (
    Config,
    Facts,
    GetConfigInput,
    GetFactsInput,
    GetInterfacesInput,
    GetLldpNeighborsInput,
    Interfaces,
    LldpNeighbors,
    SetInterfaceDescriptionsInput,
    SetInterfaceDescriptionsOutput,
)
from nornir_sandbox.operations.parse import (
    description_changes,
    description_result,
    srlinux_facts,
    srlinux_interface_descriptions,
    srlinux_interfaces,
    srlinux_lldp_neighbors,
)
from nornir_sandbox.srlinux import configure, configure_sync, send, send_json, send_json_sync, send_sync

LLDP_NEIGHBORS = "info from state /system lldp interface * neighbor *"
# admin-state makes SR Linux list the interfaces that have no description.
INTERFACES = "info from state /interface * | filter fields description admin-state oper-state mtu"
DESCRIPTIONS = "info from state /interface * | filter fields description admin-state"


def _description_lines(changes: dict[str, str]) -> list[str]:
    return [
        f'set / interface {interface} description "{description}"'
        if description
        else f"delete / interface {interface} description"
        for interface, description in changes.items()
    ]


async def get_facts(task: Task, params: GetFactsInput) -> Facts:
    return srlinux_facts(await send_json(task, "show version"))


def get_facts_sync(task: Task, params: GetFactsInput) -> Facts:
    return srlinux_facts(send_json_sync(task, "show version"))


async def get_config(task: Task, params: GetConfigInput) -> Config:
    return Config(config=await send(task, "info from running /"))


def get_config_sync(task: Task, params: GetConfigInput) -> Config:
    return Config(config=send_sync(task, "info from running /"))


async def get_lldp_neighbors(task: Task, params: GetLldpNeighborsInput) -> LldpNeighbors:
    return srlinux_lldp_neighbors(await send_json(task, LLDP_NEIGHBORS))


def get_lldp_neighbors_sync(task: Task, params: GetLldpNeighborsInput) -> LldpNeighbors:
    return srlinux_lldp_neighbors(send_json_sync(task, LLDP_NEIGHBORS))


async def set_interface_descriptions(
    task: Task, params: SetInterfaceDescriptionsInput
) -> Result[SetInterfaceDescriptionsOutput]:
    current = srlinux_interface_descriptions(await send_json(task, DESCRIPTIONS))
    changes = description_changes(current, params.descriptions)
    if changes:
        await configure(task, _description_lines(changes))
    return description_result(task, current, params.descriptions)


def set_interface_descriptions_sync(
    task: Task, params: SetInterfaceDescriptionsInput
) -> Result[SetInterfaceDescriptionsOutput]:
    current = srlinux_interface_descriptions(send_json_sync(task, DESCRIPTIONS))
    changes = description_changes(current, params.descriptions)
    if changes:
        configure_sync(task, _description_lines(changes))
    return description_result(task, current, params.descriptions)


async def get_interfaces(task: Task, params: GetInterfacesInput) -> Interfaces:
    return srlinux_interfaces(await send_json(task, INTERFACES), params)


def get_interfaces_sync(task: Task, params: GetInterfacesInput) -> Interfaces:
    return srlinux_interfaces(send_json_sync(task, INTERFACES), params)
