"""Run the get_facts Operation on every host with nr.arun_operation() on the asyncio runner.

The loop has no condition on the platform: each host runs the task registered for its
platform (arista_eos or nokia_srlinux), and `res.result` is a Facts instance for both.
"""

from __future__ import annotations

from cyclopts import App

from nornir_sandbox import init_nornir
from nornir_sandbox.cli import Hosts, describe_result
from nornir_sandbox.operations import GetFacts

app = App()


@app.default
async def main(*, hosts: Hosts = None) -> int:
    """Print the vendor, model, software version and serial number of each device."""
    nr = await init_nornir(hosts=hosts)
    async with nr:
        print(f"{'host':<8} {'vendor':<8} {'model':<14} {'version':<45} serial")
        async with nr.arun_operation(GetFacts) as stream:
            async for res in stream.results():
                if res.failed:
                    print(f"{res.host!s:<8} FAILED   {describe_result(res)}")
                    continue
                facts = res.result
                print(
                    f"{res.host!s:<8} {facts.vendor:<8} {facts.model:<14} {facts.os_version:<45} {facts.serial_number}"
                )
            result = await stream.result()
    return int(result.failed)


if __name__ == "__main__":
    app()
