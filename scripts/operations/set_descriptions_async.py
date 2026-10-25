"""Set the description of every link interface with the set_interface_descriptions
Operation, on the asyncio runner.

Each host gets its own input: `params` is a function that Nornir calls with the host
when the host starts, and that builds the descriptions from the host's links in
inventory/hosts.yaml. The result of each host reports `changed` and a `diff`; a second
run with the same template changes nothing.
"""

from __future__ import annotations

from cyclopts import App
from nornir.core.inventory import Host

from nornir_sandbox import init_nornir
from nornir_sandbox.checks import DESCRIPTION_TEMPLATE, link_descriptions, links_from
from nornir_sandbox.cli import Hosts, describe_result
from nornir_sandbox.operations import SetInterfaceDescriptions, SetInterfaceDescriptionsInput

app = App()


@app.default
async def main(*, hosts: Hosts = None, template: str = DESCRIPTION_TEMPLATE) -> int:
    """Set the description of each link interface from a template.

    Example: set_descriptions_async.py --template "link to {peer} {peer_interface}"
    Run it again without --template to restore the descriptions of the startup
    configurations.

    Parameters
    ----------
    template
        Description of each link interface, with the fields {interface}, {peer} and
        {peer_interface}.
    """

    def descriptions(host: Host) -> SetInterfaceDescriptionsInput:
        return SetInterfaceDescriptionsInput(
            descriptions=link_descriptions(links_from(host.get("links", [])), template)
        )

    nr = await init_nornir(hosts=hosts)
    async with nr, nr.arun_operation(SetInterfaceDescriptions, descriptions) as stream:
        async for res in stream.results():
            if res.failed:
                print(f"{res.host!s:<8} FAILED     {describe_result(res)}")
                continue
            status = "changed" if res.changed else "no change"
            print(f"{res.host!s:<8} {status:<10} {len(res.result.previous)} interfaces")
            for line in res.diff.splitlines():
                print(f"{'':<8} {line}")
        result = await stream.result()
    return int(result.failed)


if __name__ == "__main__":
    app()
