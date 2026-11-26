"""Run the get_facts Operation on every host with nr.run_operation() on the threaded or
serial runner.

The sync counterpart of get_facts_async.py. init_nornir_sync() sets the operation connection
to `scrapli_sync`, so each host runs the `def` task registered for its platform.
"""

from __future__ import annotations

from cyclopts import App

from nornir_sandbox import init_nornir_sync
from nornir_sandbox.cli import Hosts, SyncRunner, describe_result
from nornir_sandbox.operations import GetFacts

app = App()


@app.default
def main(*, hosts: Hosts = None, runner: SyncRunner = "threaded") -> int:
    """Print the vendor, model, software version and serial number of each device."""
    with init_nornir_sync(hosts=hosts, runner=runner) as nr:
        print(f"{'host':<8} {'vendor':<8} {'model':<14} {'version':<45} serial")
        with nr.run_operation(GetFacts) as stream:
            for res in stream.results():
                if res.failed:
                    print(f"{res.host!s:<8} FAILED   {describe_result(res)}")
                    continue
                facts = res.result
                print(
                    f"{res.host!s:<8} {facts.vendor:<8} {facts.model:<14} {facts.os_version:<45} {facts.serial_number}"
                )
            result = stream.result()
    return int(result.failed)


if __name__ == "__main__":
    app()
