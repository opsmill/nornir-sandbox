"""The static inventory must describe the lab defined in lab/sandbox.clab.yml.

When the two files drift apart, scripts connect to the wrong address or the
validation reports links that are not cabled.
"""

from __future__ import annotations

import re

import pytest
import yaml

from nornir_sandbox import init_nornir
from nornir_sandbox.paths import REPO_ROOT

TOPOLOGY_FILE = REPO_ROOT / "lab" / "sandbox.clab.yml"


def _eos_name(clab_interface: str) -> str:
    return re.sub(r"^eth(\d+)$", r"Ethernet\1", clab_interface)


def test_hosts_and_addresses_match_the_lab() -> None:
    nodes = yaml.safe_load(TOPOLOGY_FILE.read_text())["topology"]["nodes"]
    hosts = init_nornir().inventory.hosts

    assert {name: host.hostname for name, host in hosts.items()} == {
        name: node["mgmt-ipv4"] for name, node in nodes.items()
    }


def test_links_match_the_lab_cabling() -> None:
    links = yaml.safe_load(TOPOLOGY_FILE.read_text())["topology"]["links"]
    cabled = set()
    for link in links:
        (a, a_intf), (b, b_intf) = (endpoint.split(":") for endpoint in link["endpoints"])
        cabled.add((a, _eos_name(a_intf), b, _eos_name(b_intf)))
        cabled.add((b, _eos_name(b_intf), a, _eos_name(a_intf)))

    declared = {
        (name, link["interface"], link["peer"], link["peer_interface"])
        for name, host in init_nornir().inventory.hosts.items()
        for link in host.get("links", [])
    }

    assert declared == cabled


def test_host_filter_rejects_unknown_names() -> None:
    with pytest.raises(ValueError, match="ceos9"):
        init_nornir(hosts=["ceos1", "ceos9"])


def test_host_filter_matches_exact_names() -> None:
    assert set(init_nornir(hosts=["ceos1"]).inventory.hosts) == {"ceos1"}
