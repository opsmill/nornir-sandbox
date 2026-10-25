from __future__ import annotations

from typing import Annotated, Literal

from cyclopts import App, Parameter
from nornir.core.task import Result, Task

from nornir_sandbox import close_failed_connections, init_nornir
from nornir_sandbox.checks import Link, compare_interfaces, compare_lldp
from nornir_sandbox.cli import EosHosts, Verbose, print_summary
from nornir_sandbox.eos import PLATFORM, send_json


def _links(task: Task) -> list[Link]:
    return [Link(**link) for link in task.host.get("links", [])]


def _result(task: Task, problems: list[str], ok_message: str) -> Result:
    if problems:
        return Result(host=task.host, failed=True, result="; ".join(problems))
    return Result(host=task.host, result=ok_message)


async def validate_interfaces(task: Task) -> Result:
    """Every interface listed in the host's links exists and has line protocol up."""
    expected = [link.interface for link in _links(task)]
    interfaces = (await send_json(task, "show interfaces")).get("interfaces", {})
    return _result(task, compare_interfaces(expected, interfaces), f"{len(expected)} link interfaces up")


async def validate_lldp(task: Task) -> Result:
    """LLDP neighbors match the host's links, with no extra neighbors."""
    expected = _links(task)
    neighbors = (await send_json(task, "show lldp neighbors")).get("lldpNeighbors", [])
    return _result(task, compare_lldp(expected, neighbors), f"{len(expected)} LLDP neighbors match the inventory")


CHECKS = {
    "interfaces": validate_interfaces,
    "lldp": validate_lldp,
}

app = App()


@app.default
async def main(
    *,
    hosts: EosHosts = None,
    check: Annotated[list[Literal["interfaces", "lldp"]] | None, Parameter(negative=())] = None,
    verbose: Verbose = False,
) -> int:
    """Check that the lab devices match the links in inventory/hosts.yaml.

    LLDP neighbors can take about 30 seconds to appear after the devices boot.

    Parameters
    ----------
    check
        Check to run; repeat the option for several checks. Default: all.
        interfaces: every link interface has line protocol up.
        lldp: LLDP neighbors match the links, with no extra neighbors.
    """
    nr = await init_nornir(hosts=hosts, platform=PLATFORM)
    async with nr:
        # on_failed=True: a host that fails one check still runs the others.
        results = []
        for name in check or CHECKS:
            result = await nr.arun(task=CHECKS[name], on_failed=True)
            await close_failed_connections(nr, result)
            results.append(result)
    return print_summary(results, verbose=verbose)


if __name__ == "__main__":
    app()
