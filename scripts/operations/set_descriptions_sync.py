"""Set the description of every link interface with the set_interface_descriptions
Operation, on the threaded or serial runner.

The sync counterpart of set_descriptions_async.py.
"""

from __future__ import annotations

from cyclopts import App
from nornir.core.inventory import Host

from nornir_sandbox import init_nornir_sync
from nornir_sandbox.checks import DESCRIPTION_TEMPLATE, link_descriptions, links_from
from nornir_sandbox.cli import Hosts, SyncRunner, describe_result
from nornir_sandbox.operations import SetInterfaceDescriptions, SetInterfaceDescriptionsInput

app = App()


@app.default
def main(*, hosts: Hosts = None, template: str = DESCRIPTION_TEMPLATE, runner: SyncRunner = "threaded") -> int:
    """Set the description of each link interface from a template.

    Example: set_descriptions_sync.py --template "link to {peer} {peer_interface}"
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

    with (
        init_nornir_sync(hosts=hosts, runner=runner) as nr,
        nr.run_operation(SetInterfaceDescriptions, descriptions) as stream,
    ):
        for res in stream.results():
            if res.failed:
                print(f"{res.host!s:<8} FAILED     {describe_result(res)}")
                continue
            status = "changed" if res.changed else "no change"
            print(f"{res.host!s:<8} {status:<10} {len(res.result.previous)} interfaces")
            for line in res.diff.splitlines():
                print(f"{'':<8} {line}")
        result = stream.result()
    return int(result.failed)


if __name__ == "__main__":
    app()
