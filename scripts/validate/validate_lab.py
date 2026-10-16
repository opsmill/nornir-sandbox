from __future__ import annotations

from typing import Annotated, Literal

from cyclopts import App, Parameter

from nornir_sandbox import init_nornir
from nornir_sandbox.cli import Hosts, Verbose, print_summary
from nornir_sandbox.tasks import validate_interfaces, validate_lldp

CHECKS = {
    "interfaces": validate_interfaces,
    "lldp": validate_lldp,
}

app = App()


@app.default
async def main(
    *,
    hosts: Hosts = None,
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
    async with init_nornir(hosts=hosts) as nr:
        # on_failed=True: a host that fails one check still runs the others.
        results = [await nr.arun(task=CHECKS[name], on_failed=True) for name in check or CHECKS]
    return print_summary(results, verbose=verbose)


if __name__ == "__main__":
    app()
