"""The static inventory must describe the lab defined in lab/sandbox.clab.yml.

When the two files drift apart, scripts connect to the wrong address or the
validation reports links that are not cabled.
"""

from __future__ import annotations

import asyncio
import re

import pytest
import yaml
from nornir.core.inventory import Host
from nornir.core.run import RunOptions
from nornir.core.task import AggregatedResult, MultiResult, Result, Task

from nornir_sandbox import close_failed_connections, init_nornir, init_nornir_sync
from nornir_sandbox.core import RunnerName
from nornir_sandbox.paths import REPO_ROOT

TOPOLOGY_FILE = REPO_ROOT / "lab" / "sandbox.clab.yml"


def _eos_name(clab_interface: str) -> str:
    return re.sub(r"^eth(\d+)$", r"Ethernet\1", clab_interface)


def test_hosts_and_addresses_match_the_lab() -> None:
    nodes = yaml.safe_load(TOPOLOGY_FILE.read_text())["topology"]["nodes"]
    hosts = asyncio.run(init_nornir()).inventory.hosts

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
        for name, host in asyncio.run(init_nornir()).inventory.hosts.items()
        for link in host.get("links", [])
    }

    assert declared == cabled


def test_host_filter_rejects_unknown_names() -> None:
    with pytest.raises(ValueError, match="ceos9"):
        asyncio.run(init_nornir(hosts=["ceos1", "ceos9"]))


def test_host_filter_matches_exact_names() -> None:
    assert set(asyncio.run(init_nornir(hosts=["ceos1"])).inventory.hosts) == {"ceos1"}


def test_sync_and_async_init_give_the_same_hosts_and_runner() -> None:
    sync_nr = init_nornir_sync(hosts=["ceos1", "ceos2"])
    async_nr = asyncio.run(init_nornir(hosts=["ceos1", "ceos2"]))

    assert set(sync_nr.inventory.hosts) == set(async_nr.inventory.hosts) == {"ceos1", "ceos2"}
    assert type(sync_nr.runner) is type(async_nr.runner)


def test_sync_init_rejects_unknown_names() -> None:
    with pytest.raises(ValueError, match="ceos9"):
        init_nornir_sync(hosts=["ceos9"])


@pytest.mark.parametrize(
    ("runner", "expected"), [("threaded", "ThreadedRunner"), ("serial", "SerialRunner"), ("ray", "RayRunner")]
)
def test_sync_init_selects_the_runner_and_its_deadlines(runner: RunnerName, expected: str) -> None:
    nr = init_nornir_sync(runner=runner, per_host_timeout=4, timeout=9)
    assert type(nr.runner).__name__ == expected
    assert getattr(nr.runner, "run_options", None) == RunOptions(per_host_timeout=4, timeout=9)


def test_hosts_have_a_site() -> None:
    hosts = init_nornir_sync().inventory.hosts
    assert {name: host.data.get("site") for name, host in hosts.items()} == {
        "ceos1": "site_a",
        "ceos2": "site_a",
        "ceos3": "site_b",
    }


def test_async_init_selects_the_ray_runner() -> None:
    nr = asyncio.run(init_nornir(runner="ray", per_host_timeout=4, timeout=9))
    assert type(nr.runner).__name__ == "RayRunner"
    assert getattr(nr.runner, "run_options", None) == RunOptions(per_host_timeout=4, timeout=9)


class _RecordingRunner:
    """Async runner that records the hosts of each run and fails nothing."""

    def __init__(self) -> None:
        self.runs: list[list[str]] = []

    def run(self, task: Task, hosts: list[Host]) -> AggregatedResult:
        raise NotImplementedError

    async def arun(self, task: Task, hosts: list[Host]) -> AggregatedResult:
        self.runs.append(sorted(host.name for host in hosts))
        return AggregatedResult(task.name)


def test_close_failed_connections_runs_through_the_runner() -> None:
    runner = _RecordingRunner()
    nr = asyncio.run(init_nornir()).with_runner(runner)
    result = AggregatedResult("t")
    for name in ("ceos1", "ceos2", "ceos3"):
        multi = MultiResult("t")
        multi.append(Result(nr.inventory.hosts[name], failed=name != "ceos2"))
        result[name] = multi

    asyncio.run(close_failed_connections(nr, result))

    assert runner.runs == [["ceos1", "ceos3"]]
