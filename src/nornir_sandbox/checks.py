"""Compare device state with the links declared in the Nornir inventory.

These functions take already-parsed device output, so they can be tested
without a running lab. Each returns a list of problems; an empty list means
the check passed.
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


def _short_name(device: str) -> str:
    """LLDP reports the system name, which may include a domain (ceos2.lab.local)."""
    return device.split(".", 1)[0]


def compare_lldp(expected: Iterable[Link], neighbors: Iterable[Mapping[str, Any]]) -> list[str]:
    """Check LLDP neighbors against the expected links.

    `neighbors` is the `lldpNeighbors` list from EOS `show lldp neighbors | json`:
    entries with `port`, `neighborDevice` and `neighborPort`. Neighbors seen on
    management interfaces are ignored, because every lab node shares the
    management network.
    """
    seen: dict[str, set[tuple[str, str]]] = {}
    for entry in neighbors:
        port = str(entry.get("port", ""))
        if port.startswith("Management"):
            continue
        peer = (_short_name(str(entry.get("neighborDevice", ""))), str(entry.get("neighborPort", "")))
        seen.setdefault(port, set()).add(peer)

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
