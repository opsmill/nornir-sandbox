"""The parse functions turn EOS and SR Linux JSON into the same Operation models.

The samples are trimmed output of the lab devices.
"""

from __future__ import annotations

import pytest
from nornir.core.inventory import Host
from nornir.core.task import Task

from nornir_sandbox.checks import LldpNeighbor
from nornir_sandbox.operations import Facts, SetInterfaceDescriptionsOutput
from nornir_sandbox.operations.parse import (
    UnknownInterfaceError,
    description_changes,
    description_result,
    eos_facts,
    eos_interface_descriptions,
    eos_lldp_neighbors,
    srlinux_facts,
    srlinux_interface_descriptions,
    srlinux_lldp_neighbors,
)

EOS_VERSION = {
    "modelName": "cEOSLab",
    "mfgName": "Arista",
    "serialNumber": "86E702FD945332D4D44677EAB151DC2F",
    "version": "4.28.9M-33818481.4289M (engineering build)",
}
SRL_VERSION = {
    "basic system info": {
        "Hostname": "srl1",
        "Chassis Type": "7220 IXR-D2L",
        "Serial Number": "Sim Serial No.",
        "Software Version": "v24.10.1",
    }
}
SRL_LLDP = {
    "system": {
        "lldp": {
            "interface": [
                {"name": "ethernet-1/1", "neighbor": [{"system-name": "ceos1", "port-id": "Ethernet3"}]},
                {"name": "mgmt0", "neighbor": [{"system-name": "ceos2", "port-id": "Management0"}]},
                {"name": "ethernet-1/3"},
            ]
        }
    }
}


def test_eos_and_sr_linux_facts() -> None:
    assert eos_facts(EOS_VERSION, {"hostname": "ceos1", "fqdn": "ceos1"}) == Facts(
        hostname="ceos1",
        vendor="Arista",
        model="cEOSLab",
        os_version="4.28.9M-33818481.4289M (engineering build)",
        serial_number="86E702FD945332D4D44677EAB151DC2F",
    )
    assert srlinux_facts(SRL_VERSION) == Facts(
        hostname="srl1", vendor="Nokia", model="7220 IXR-D2L", os_version="v24.10.1", serial_number="Sim Serial No."
    )


def test_eos_and_sr_linux_lldp_neighbors() -> None:
    eos = {
        "lldpNeighbors": [{"port": "Ethernet3", "neighborDevice": "srl1", "neighborPort": "ethernet-1/1", "ttl": 120}]
    }

    assert eos_lldp_neighbors(eos).neighbors == (LldpNeighbor("Ethernet3", "srl1", "ethernet-1/1"),)
    assert srlinux_lldp_neighbors(SRL_LLDP).neighbors == (
        LldpNeighbor("ethernet-1/1", "ceos1", "Ethernet3"),
        LldpNeighbor("mgmt0", "ceos2", "Management0"),
    )


def test_interface_descriptions_include_interfaces_without_one() -> None:
    eos = {"interfaces": {"Ethernet1": {"description": "to ceos2 Ethernet1"}, "Ethernet9": {}}}
    srl = {"interface": [{"name": "ethernet-1/1", "description": "to ceos1 Ethernet3"}, {"name": "ethernet-1/3"}]}

    assert eos_interface_descriptions(eos) == {"Ethernet1": "to ceos2 Ethernet1", "Ethernet9": ""}
    assert srlinux_interface_descriptions(srl) == {"ethernet-1/1": "to ceos1 Ethernet3", "ethernet-1/3": ""}


def test_description_changes_keep_only_the_differences() -> None:
    current = {"Ethernet1": "to ceos2 Ethernet1", "Ethernet2": "old"}

    assert description_changes(current, {"Ethernet1": "to ceos2 Ethernet1", "Ethernet2": "new"}) == {"Ethernet2": "new"}


def test_description_changes_reject_unknown_interfaces() -> None:
    with pytest.raises(UnknownInterfaceError, match="Ethernet7, Ethernet9"):
        description_changes({"Ethernet1": ""}, {"Ethernet9": "x", "Ethernet7": "y", "Ethernet1": ""})


def _task(host: Host) -> Task:
    task = Task(lambda task: None, nornir=None, global_dry_run=False, processors=None)  # type: ignore[arg-type]
    task.host = host
    return task


def test_description_result_reports_changed_and_diff() -> None:
    task = _task(Host("ceos1"))
    current = {"Ethernet1": "to ceos2 Ethernet1", "Ethernet2": "old", "Ethernet3": ""}

    changed = description_result(task, current, {"Ethernet1": "to ceos2 Ethernet1", "Ethernet2": "new"})
    unchanged = description_result(task, current, {"Ethernet1": "to ceos2 Ethernet1"})

    assert changed.changed
    assert changed.diff == "-Ethernet2 description 'old'\n+Ethernet2 description 'new'"
    assert changed.result == SetInterfaceDescriptionsOutput(
        previous={"Ethernet1": "to ceos2 Ethernet1", "Ethernet2": "old"}
    )
    assert not unchanged.changed
    assert unchanged.diff == ""
