"""Pure functions that turn the JSON output of EOS and SR Linux into the Operation models.

They take decoded `| json` (EOS) or `| as json` (SR Linux) output, so they can be
tested without the lab.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from nornir.core.task import Result, Task

from nornir_sandbox.checks import LldpNeighbor
from nornir_sandbox.operations.contracts import (
    Facts,
    LldpNeighbors,
    SetInterfaceDescriptionsOutput,
)


class UnknownInterfaceError(LookupError):
    """A requested interface does not exist on the device."""

    def __init__(self, interfaces: list[str]) -> None:
        super().__init__(f"interface(s) not found on the device: {', '.join(interfaces)}")
        self.interfaces = interfaces


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
