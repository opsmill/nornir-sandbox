"""get_interfaces uses Pydantic models: Nornir validates the input with
model_validate(), and the parse functions map both platforms to the same output model.

The samples are trimmed output of ceos1 and of an SR Linux 24.10.1 container.
"""

from __future__ import annotations

from typing import Any

import pytest
from nornir.core.exceptions import OperationInputError
from pydantic import ValidationError

from nornir_sandbox import init_nornir_sync
from nornir_sandbox.operations import GetInterfaces, GetInterfacesInput, Interface, Interfaces
from nornir_sandbox.operations.parse import eos_interfaces, srlinux_interfaces

EOS_INTERFACES = {
    "interfaces": {
        "Management0": {
            "name": "Management0",
            "description": "",
            "interfaceStatus": "connected",
            "lineProtocolStatus": "up",
            "mtu": 1500,
        },
        "Ethernet1": {
            "name": "Ethernet1",
            "description": "to ceos2 Ethernet1",
            "interfaceStatus": "connected",
            "lineProtocolStatus": "up",
            "mtu": 1500,
        },
        "Ethernet3": {
            "name": "Ethernet3",
            "description": "",
            "interfaceStatus": "disabled",
            "lineProtocolStatus": "down",
            "mtu": 1500,
        },
    }
}
SRL_INTERFACES = {
    "interface": [
        {
            "name": "ethernet-1/1",
            "description": "to ceos1 Ethernet3",
            "admin-state": "enable",
            "mtu": 9232,
            "oper-state": "up",
        },
        {"name": "ethernet-1/2", "admin-state": "disable", "oper-state": "down"},
        {"name": "mgmt0", "admin-state": "enable", "mtu": 1514, "oper-state": "up"},
    ]
}


def test_eos_and_sr_linux_interfaces_map_to_the_same_model() -> None:
    eos = eos_interfaces(EOS_INTERFACES, GetInterfacesInput())
    srl = srlinux_interfaces(SRL_INTERFACES, GetInterfacesInput())

    assert eos.interfaces[1] == Interface(
        name="Ethernet1", description="to ceos2 Ethernet1", admin_state="up", oper_state="up", mtu=1500
    )
    assert eos.interfaces[2].admin_state == "down"
    assert srl.interfaces[0] == Interface(
        name="ethernet-1/1", description="to ceos1 Ethernet3", admin_state="up", oper_state="up", mtu=9232
    )
    # SR Linux reports no description and no MTU for a disabled interface.
    assert srl.interfaces[1] == Interface(name="ethernet-1/2", admin_state="down", oper_state="down")


def test_oper_state_keeps_only_the_interfaces_in_that_state() -> None:
    params = GetInterfacesInput(oper_state="down")

    assert [i.name for i in eos_interfaces(EOS_INTERFACES, params).interfaces] == ["Ethernet3"]
    assert [i.name for i in srlinux_interfaces(SRL_INTERFACES, params).interfaces] == ["ethernet-1/2"]


def test_output_round_trips_through_json() -> None:
    result = srlinux_interfaces(SRL_INTERFACES, GetInterfacesInput())

    assert Interfaces.model_validate_json(result.model_dump_json()) == result


def test_output_model_rejects_values_outside_the_contract() -> None:
    with pytest.raises(ValidationError, match="admin_state"):
        Interface(name="Ethernet1", admin_state="enable", oper_state="up")  # type: ignore[arg-type]
    with pytest.raises(ValidationError, match="mtu"):
        Interface(name="Ethernet1", admin_state="up", oper_state="up", mtu=0)


@pytest.mark.parametrize(
    ("params", "message"),
    [
        ({"oper_state": "upp"}, "oper_state"),
        ({"oper_sate": "up"}, "oper_sate"),
    ],
)
def test_invalid_input_fails_before_the_run_starts(params: dict[str, Any], message: str) -> None:
    nr = init_nornir_sync(runner="serial")

    with pytest.raises(OperationInputError, match=message):
        nr.run_operation(GetInterfaces, params)
