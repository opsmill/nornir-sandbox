"""Check the LLDP neighbors of every host against inventory/hosts.yaml with the
get_lldp_neighbors Operation, on the threaded or serial runner.

The sync counterpart of validate_lldp_async.py.
"""

from __future__ import annotations

from cyclopts import App

from nornir_sandbox import init_nornir_sync
from nornir_sandbox.checks import compare_neighbors, links_from
from nornir_sandbox.cli import Hosts, SyncRunner, describe_result
from nornir_sandbox.operations import GetLldpNeighbors

app = App()


@app.default
def main(*, hosts: Hosts = None, runner: SyncRunner = "threaded") -> int:
    """Check that the LLDP neighbors of each device match its links, with no extra neighbors.

    LLDP neighbors can take about 30 seconds to appear after the devices boot.
    """
    problems_found = False
    with init_nornir_sync(hosts=hosts, runner=runner) as nr, nr.run_operation(GetLldpNeighbors) as stream:
        for res in stream.results():
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
