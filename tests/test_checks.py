from __future__ import annotations

from nornir_sandbox.checks import Link, compare_interfaces, compare_lldp, summarize_fact

EXPECTED = [Link("Ethernet1", "ceos2", "Ethernet1"), Link("Ethernet2", "ceos3", "Ethernet2")]


def neighbor(port: str, device: str, peer_port: str) -> dict[str, object]:
    return {"port": port, "neighborDevice": device, "neighborPort": peer_port, "ttl": 120}


def test_lldp_matches_and_ignores_management_neighbors() -> None:
    neighbors = [
        neighbor("Ethernet1", "ceos2.lab.local", "Ethernet1"),
        neighbor("Ethernet2", "ceos3", "Ethernet2"),
        neighbor("Management0", "ceos2", "Management0"),
    ]

    assert compare_lldp(EXPECTED, neighbors) == []


def test_lldp_reports_missing_wrong_and_unexpected_neighbors() -> None:
    neighbors = [
        neighbor("Ethernet2", "ceos2", "Ethernet2"),
        neighbor("Ethernet3", "ceos9", "Ethernet1"),
    ]

    assert compare_lldp(EXPECTED, neighbors) == [
        "Ethernet1: no LLDP neighbor, expected ceos2 Ethernet1",
        "Ethernet2: neighbor is ceos2 Ethernet2, expected ceos3 Ethernet2",
        "Ethernet3: unexpected LLDP neighbor ceos9 Ethernet1",
    ]


def test_interfaces_report_down_and_missing() -> None:
    interfaces = {
        "Ethernet1": {"lineProtocolStatus": "up", "interfaceStatus": "connected"},
        "Ethernet2": {"lineProtocolStatus": "down", "interfaceStatus": "disabled"},
    }

    assert compare_interfaces(["Ethernet1", "Ethernet2", "Ethernet3"], interfaces) == [
        "Ethernet2: line protocol is down (interface status disabled)",
        "Ethernet3: not present on the device",
    ]


def test_summaries() -> None:
    interfaces = {
        "interfaces": {"Ethernet1": {"lineProtocolStatus": "up"}, "Ethernet2": {"lineProtocolStatus": "down"}},
    }

    assert summarize_fact("show version", {"version": "4.28.9M"}) == "EOS 4.28.9M"
    assert summarize_fact("show interfaces", interfaces) == "1/2 interfaces up"
    assert summarize_fact("show lldp neighbors", {"lldpNeighbors": [{}, {}]}) == "2 LLDP neighbors"
