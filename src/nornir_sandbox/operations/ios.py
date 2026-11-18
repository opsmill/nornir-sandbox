from __future__ import annotations

from nornir.core.task import Result, Task

from nornir_sandbox.ios import (
    send,
    send_sync,
    send_and_parse,   
    send_and_parse_sync,   
)
from nornir_sandbox.operations.contracts import (
    Config,
    Facts,
    GetConfigInput,
    GetFactsInput,
)
from nornir_sandbox.operations.parse import (
    ios_facts,
    ios_interfaces,
    ios_lldp_neighbors
)


async def get_facts(task: Task, params: GetFactsInput) -> Facts:
   return ios_facts(await send_and_parse(task, "show version"))


async def get_config(task: Task, params: GetConfigInput) -> Config:
    return Config(config=await send(task, "show running-config"))


async def get_interfaces(task: Task, params: GetInterfacesInput) -> Interfaces:
    return ios_interfaces(await send_and_parse(task, "show interfaces"), params)


async def get_lldp_neighbors(task: Task, params: GetLldpNeighborsInput) -> LldpNeighbors:
    return ios_lldp_neighbors(await send_and_parse(task, "show lldp neighbors"))


def get_facts_sync(task: Task, params: GetFactsInput) -> Facts:
    return ios_facts(send_and_parse_sync(task, "show version"))


def get_config_sync(task: Task, params: GetConfigInput) -> Config:
    return Config(config=send_sync(task, "show running-config"))


def get_interfaces_sync(task: Task, params: GetInterfacesInput) -> Interfaces:
    return ios_interfaces(send_and_parse_sync(task, "show interfaces"), params)


def get_lldp_neighbors_sync(task: Task, params: GetLldpNeighborsInput) -> LldpNeighbors:
    return ios_lldp_neighbors(send_and_parse_sync(task, "show lldp neighbors"))
