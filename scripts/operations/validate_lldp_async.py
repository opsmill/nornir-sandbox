"""Check the LLDP neighbors of every host against inventory/hosts.yaml with the
get_lldp_neighbors Operation, on the asyncio runner.

EOS and SR Linux report LLDP neighbors in different JSON; their tasks return the same
LldpNeighbors model, so one check, compare_neighbors(), covers both platforms.
"""

from __future__ import annotations

from cyclopts import App

from nornir_sandbox import init_nornir
from nornir_sandbox.checks import compare_neighbors, links_from
from nornir_sandbox.cli import Hosts, describe_result
from nornir_sandbox.operations import GetLldpNeighbors

app = App()


@app.default
async def main(*, hosts: Hosts = None) -> int:
    """Check that the LLDP neighbors of each device match its links, with no extra neighbors.

    LLDP neighbors can take about 30 seconds to appear after the devices boot.
    """
    problems_found = False
    nr = await init_nornir(hosts=hosts)
    async with nr, nr.arun_operation(GetLldpNeighbors) as stream:
        async for res in stream.results():
            if res.failed:
                problems_found = True
                print(f"{res.host!s:<8} FAILED  {describe_result(res)}")
                continue
            expected = links_from(res.host.get("links", [])) if res.host else []
            problems = compare_neighbors(expected, res.result.neighbors)
            problems_found |= bool(problems)
            status = "FAILED " if problems else "OK     "
            print(f"{res.host!s:<8} {status} {'; '.join(problems) or f'{len(expected)} LLDP neighbors match'}")
    return int(problems_found)


if __name__ == "__main__":
    app()
