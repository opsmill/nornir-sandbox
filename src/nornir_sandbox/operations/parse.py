"""Pure functions that turn the JSON output of EOS and SR Linux into the Operation models.

They take decoded `| json` (EOS) or `| as json` (SR Linux) output, so they can be
tested without the lab.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from netutils.interface import canonical_interface_name
from nornir.core.task import Result, Task

from nornir_sandbox.checks import LldpNeighbor
from nornir_sandbox.operations.contracts import (
    Facts,
    GetInterfacesInput,
    Interface,
    Interfaces,
    LldpNeighbors,
    SetInterfaceDescriptionsOutput,
)


class UnknownInterfaceError(LookupError):
    """A requested interface does not exist on the device."""

    def __init__(self, interfaces: list[str]) -> None:
        super().__init__(f"interface(s) not found on the device: {', '.join(interfaces)}")
        self.interfaces = interfaces


def ios_facts(version: Mapping[str, Any]) -> Facts:
    return Facts(
        hostname=str(version[0]["hostname"]),
        vendor="Cisco",
        model=str(version[0]["hardware"][0]),
        os_version=str(version[0]["version"]),
        serial_number=str(version[0]["serial"][0]),
    )


def eos_facts(version: Mapping[str, Any], hostname: Mapping[str, Any]) -> Facts:
    """Build Facts from EOS `show version | json` and `show hostname | json`."""
    return Facts(
        hostname=str(hostname["hostname"]),
        vendor=str(version.get("mfgName") or "Arista"),
        model=str(version["modelName"]),
        os_version=str(version["version"]),
        serial_number=str(version.get("serialNumber", "")),
    )


def srlinux_facts(version: Mapping[str, Any]) -> Facts:
    """Build Facts from SR Linux `show version | as json`."""
    info = version["basic system info"]
    return Facts(
        hostname=str(info["Hostname"]),
        vendor="Nokia",
        model=str(info["Chassis Type"]),
        os_version=str(info["Software Version"]),
        serial_number=str(info.get("Serial Number", "")),
    )


def ios_lldp_neighbors(neighbors: list[Mapping[str, Any]]) -> LldpNeighbors:
    """Build LldpNeighbors from EOS `show lldp neighbors | json`."""
    return LldpNeighbors(
        neighbors=tuple(
            LldpNeighbor(
                interface=canonical_interface_name(str(neighbor["local_interface"])),
                peer=str(neighbor["neighbor_name"]),
                peer_interface=canonical_interface_name(str(neighbor["neighbor_interface"])),
            )
            for neighbor in neighbors
        )
    )


def eos_lldp_neighbors(data: Mapping[str, Any]) -> LldpNeighbors:
    """Build LldpNeighbors from EOS `show lldp neighbors | json`."""
    return LldpNeighbors(
        neighbors=tuple(
            LldpNeighbor(
                interface=str(entry["port"]),
                peer=str(entry["neighborDevice"]),
                peer_interface=str(entry["neighborPort"]),
            )
            for entry in data.get("lldpNeighbors", [])
        )
    )


def srlinux_lldp_neighbors(data: Mapping[str, Any]) -> LldpNeighbors:
    """Build LldpNeighbors from SR Linux `info from state /system lldp interface * neighbor * | as json`."""
    return LldpNeighbors(
        neighbors=tuple(
            LldpNeighbor(
                interface=str(interface["name"]),
                peer=str(neighbor.get("system-name", "")),
                peer_interface=str(neighbor.get("port-id", "")),
            )
            for interface in data.get("system", {}).get("lldp", {}).get("interface", [])
            for neighbor in interface.get("neighbor", [])
        )
    )


def eos_interface_descriptions(data: Mapping[str, Any]) -> dict[str, str]:
    """Return the description of every interface from EOS `show interfaces | json`."""
    return {name: str(state.get("description", "")) for name, state in data["interfaces"].items()}


def srlinux_interface_descriptions(data: Mapping[str, Any]) -> dict[str, str]:
    """Return the description of every interface from SR Linux
    `info from state /interface * | filter fields description admin-state | as json`.

    The query includes admin-state because SR Linux leaves out the interfaces that have
    no description when only description is requested.
    """
    return {str(entry["name"]): str(entry.get("description", "")) for entry in data.get("interface", [])}


def ios_interfaces(interfaces: list[Mapping[str, Any]], params: GetInterfacesInput) -> Interfaces:
    return _select_interfaces(
        (
            Interface(
                name=interface["interface"],
                description=interface["description"],
                oper_state="up"
                if interface["link_status"] == "up" and interface["protocol_status"] == "up"
                else "down",
                admin_state="up" if interface["link_status"] != "administratively down" else "down",
                mtu=interface["mtu"] or None,
            )
            for interface in interfaces
        ),
        params,
    )


def eos_interfaces(data: Mapping[str, Any], params: GetInterfacesInput) -> Interfaces:
    """Build Interfaces from EOS `show interfaces | json`.

    EOS reports `interfaceStatus` "disabled" for an interface that is shut down, and
    `lineProtocolStatus` "up" for an interface that is operational.
    """
    return _select_interfaces(
        (
            Interface(
                name=name,
                description=str(state.get("description", "")),
                admin_state="down" if state.get("interfaceStatus") == "disabled" else "up",
                oper_state="up" if state.get("lineProtocolStatus") == "up" else "down",
                mtu=state.get("mtu"),
            )
            for name, state in data["interfaces"].items()
        ),
        params,
    )


def srlinux_interfaces(data: Mapping[str, Any], params: GetInterfacesInput) -> Interfaces:
    """Build Interfaces from SR Linux
    `info from state /interface * | filter fields description admin-state oper-state mtu | as json`.

    SR Linux reports `admin-state` "enable" or "disable", and leaves out `mtu` and
    `description` when they are not set.
    """
    return _select_interfaces(
        (
            Interface(
                name=str(entry["name"]),
                description=str(entry.get("description", "")),
                admin_state="up" if entry.get("admin-state") == "enable" else "down",
                oper_state="up" if entry.get("oper-state") == "up" else "down",
                mtu=entry.get("mtu"),
            )
            for entry in data.get("interface", [])
        ),
        params,
    )


def _select_interfaces(interfaces: Iterable[Interface], params: GetInterfacesInput) -> Interfaces:
    return Interfaces(
        interfaces=tuple(i for i in interfaces if params.oper_state is None or i.oper_state == params.oper_state)
    )


def description_changes(current: Mapping[str, str], requested: Mapping[str, str]) -> dict[str, str]:
    """Return the requested descriptions that differ from the current ones.

    Raises UnknownInterfaceError naming the requested interfaces that are not in `current`.
    """
    missing = sorted(set(requested) - set(current))
    if missing:
        raise UnknownInterfaceError(missing)
    return {name: text for name, text in requested.items() if current[name] != text}


def description_result(
    task: Task, current: Mapping[str, str], requested: Mapping[str, str]
) -> Result[SetInterfaceDescriptionsOutput]:
    """Return the result of set_interface_descriptions: the previous description of each
    requested interface, `changed`, and a diff of the descriptions that changed."""
    changes = description_changes(current, requested)
    diff = "\n".join(
        f"-{name} description {current[name]!r}\n+{name} description {text!r}" for name, text in changes.items()
    )
    return Result(
        host=task.host,
        result=SetInterfaceDescriptionsOutput(previous={name: current[name] for name in requested}),
        changed=bool(changes),
        diff=diff,
    )
