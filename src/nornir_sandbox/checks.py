"""Pure functions on parsed EOS output: compare it with the links declared in the
Nornir inventory, and summarize it.

These functions take already-parsed device output, so they can be tested
without a running lab. The compare_* functions return a list of problems; an
empty list means the check passed.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Link:
    """One lab link seen from the local host, as listed under `data.links` in inventory/hosts.yaml."""

    interface: str
    peer: str
    peer_interface: str


@dataclass(frozen=True)
class LldpNeighbor:
    """One LLDP neighbor seen by the local host: local interface, neighbor system name and port."""

    interface: str
    peer: str
    peer_interface: str


# Every lab node shares the management network, so neighbors seen there are ignored.
MANAGEMENT_INTERFACES = ("Management", "mgmt")


def _short_name(device: str) -> str:
    """LLDP reports the system name, which may include a domain (ceos2.lab.local)."""
    return device.split(".", 1)[0]


def compare_lldp(expected: Iterable[Link], neighbors: Iterable[Mapping[str, Any]]) -> list[str]:
    """Check LLDP neighbors against the expected links.

    `neighbors` is the `lldpNeighbors` list from EOS `show lldp neighbors | json`:
    entries with `port`, `neighborDevice` and `neighborPort`.
    """
    return compare_neighbors(
        expected,
        (
            LldpNeighbor(
                interface=str(entry.get("port", "")),
                peer=str(entry.get("neighborDevice", "")),
                peer_interface=str(entry.get("neighborPort", "")),
            )
            for entry in neighbors
        ),
    )


def compare_neighbors(expected: Iterable[Link], neighbors: Iterable[LldpNeighbor]) -> list[str]:
    """Check LLDP neighbors of any platform against the expected links.

    Neighbors seen on management interfaces are ignored, because every lab node
    shares the management network.
    """
    seen: dict[str, set[tuple[str, str]]] = {}
    for neighbor in neighbors:
        if neighbor.interface.startswith(MANAGEMENT_INTERFACES):
            continue
        seen.setdefault(neighbor.interface, set()).add((_short_name(neighbor.peer), neighbor.peer_interface))

    problems: list[str] = []
    expected_ports: set[str] = set()
    for link in expected:
        expected_ports.add(link.interface)
        peers = seen.get(link.interface)
        want = f"{link.peer} {link.peer_interface}"
        if not peers:
            problems.append(f"{link.interface}: no LLDP neighbor, expected {want}")
        elif (link.peer, link.peer_interface) not in peers:
            got = ", ".join(f"{d} {p}" for d, p in sorted(peers))
            problems.append(f"{link.interface}: neighbor is {got}, expected {want}")

    for port in sorted(set(seen) - expected_ports):
        got = ", ".join(f"{d} {p}" for d, p in sorted(seen[port]))
        problems.append(f"{port}: unexpected LLDP neighbor {got}")
    return problems


def compare_interfaces(expected: Iterable[str], interfaces: Mapping[str, Mapping[str, Any]]) -> list[str]:
    """Check that every expected interface exists and its line protocol is up.

    `interfaces` is the `interfaces` mapping from EOS `show interfaces | json`.
    """
    problems: list[str] = []
    for name in expected:
        state = interfaces.get(name)
        if state is None:
            problems.append(f"{name}: not present on the device")
            continue
        protocol = state.get("lineProtocolStatus")
        if protocol != "up":
            status = state.get("interfaceStatus")
            problems.append(f"{name}: line protocol is {protocol} (interface status {status})")
    return problems


def summarize_fact(command: str, data: Mapping[str, Any]) -> str:
    """Return a one-line summary of the `| json` output of `show version`, `show interfaces`
    or `show lldp neighbors`. The runner examples in scripts/runner/ report it as progress.
    """
    if command == "show version":
        return f"EOS {data['version']}"
    if command == "show interfaces":
        interfaces = data["interfaces"].values()
        up = sum(1 for interface in interfaces if interface.get("lineProtocolStatus") == "up")
        return f"{up}/{len(interfaces)} interfaces up"
    if command == "show lldp neighbors":
        return f"{len(data['lldpNeighbors'])} LLDP neighbors"
    raise ValueError(f"no summary for {command!r}")


def links_from(data: Iterable[Mapping[str, str]]) -> list[Link]:
    """Return the links of a host from its `data.links` entries in inventory/hosts.yaml."""
    return [Link(**entry) for entry in data]


DESCRIPTION_TEMPLATE = "to {peer} {peer_interface}"
"""Description of the link interfaces in the lab startup configurations."""


def link_descriptions(links: Iterable[Link], template: str = DESCRIPTION_TEMPLATE) -> dict[str, str]:
    """Return the description of each link interface, built from `template` with the
    fields of Link: {interface}, {peer} and {peer_interface}."""
    return {
        link.interface: template.format(interface=link.interface, peer=link.peer, peer_interface=link.peer_interface)
        for link in links
    }
