"""The Operations of the sandbox: a name, an input model and an output model each.

Most models are frozen dataclasses; get_interfaces uses Pydantic models. Nornir builds
the input from what the caller passes (an instance, a mapping of its fields, or a
function of the host) and checks that each host's task returns an instance of the output
model. For a Pydantic input model, Nornir passes a mapping to `model_validate()`, so
Pydantic checks the types and values of the fields, and rejects unknown fields when the
model forbids them, before any host connects.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal

from nornir.core.operations import Operation
from pydantic import BaseModel, ConfigDict, Field

from nornir_sandbox.checks import LldpNeighbor

ASYNC_CONNECTION = "scrapli"
"""Connection name under which the `async def` tasks are registered."""

SYNC_CONNECTION = "scrapli_sync"
"""Connection name under which the `def` tasks are registered. They use the same scrapli
connection as the async tasks; a registration key holds one task, so the two forms need
two names."""


@dataclass(frozen=True)
class GetFactsInput:
    """No input."""


@dataclass(frozen=True)
class Facts:
    hostname: str
    vendor: str
    model: str
    os_version: str
    serial_number: str


class GetFacts(Operation[GetFactsInput, Facts]):
    """Read the hostname, model, software version and serial number."""

    name = "get_facts"


@dataclass(frozen=True)
class GetConfigInput:
    """No input: the running configuration."""


@dataclass(frozen=True)
class Config:
    config: str


class GetConfig(Operation[GetConfigInput, Config]):
    """Read the running configuration as text, in the format of the device CLI."""

    name = "get_config"


@dataclass(frozen=True)
class GetLldpNeighborsInput:
    """No input."""


@dataclass(frozen=True)
class LldpNeighbors:
    neighbors: tuple[LldpNeighbor, ...]


class GetLldpNeighbors(Operation[GetLldpNeighborsInput, LldpNeighbors]):
    """Read the LLDP neighbors of every interface, the management interface included."""

    name = "get_lldp_neighbors"


@dataclass(frozen=True)
class SetInterfaceDescriptionsInput:
    """The description to set on each interface, by interface name. An empty
    description removes the description."""

    descriptions: Mapping[str, str]


@dataclass(frozen=True)
class SetInterfaceDescriptionsOutput:
    """The description of each requested interface before the change."""

    previous: Mapping[str, str]


class SetInterfaceDescriptions(Operation[SetInterfaceDescriptionsInput, SetInterfaceDescriptionsOutput]):
    """Set the description of interfaces. The result reports `changed` and a `diff`; the
    task sends configuration only for the interfaces whose description differs, and
    fails without changing anything when an interface does not exist."""

    name = "set_interface_descriptions"


# get_interfaces uses Pydantic models instead of dataclasses.

InterfaceState = Literal["up", "down"]


class GetInterfacesInput(BaseModel):
    """Which interfaces to read. Nornir validates a mapping such as
    `{"oper_state": "up"}` with this model; a misspelled field or a state other than
    "up" or "down" fails the run before any host connects."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    oper_state: InterfaceState | None = None
    """Read only the interfaces in this operational state. None reads every interface."""


class Interface(BaseModel):
    """One interface, with the states of both platforms mapped to "up" and "down"."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str
    description: str = ""
    admin_state: InterfaceState
    oper_state: InterfaceState
    mtu: int | None = Field(default=None, gt=0)
    """MTU in bytes. None when the device does not report it: SR Linux reports no MTU
    for a disabled interface."""


class Interfaces(BaseModel):
    model_config = ConfigDict(frozen=True)

    interfaces: tuple[Interface, ...]


class GetInterfaces(Operation[GetInterfacesInput, Interfaces]):
    """Read the description, administrative and operational state, and MTU of every
    interface, the management interface included."""

    name = "get_interfaces"
