"""Run the get_interfaces Operation, whose models are Pydantic models, with
nr.run_operation() on the threaded or serial runner.

The sync counterpart of get_interfaces_async.py. --params takes the input of the
Operation as JSON, for example '{"oper_state": "up"}'; Nornir validates it with
GetInterfacesInput.model_validate() before any host connects. --json prints each result
with model_dump_json().
"""

from __future__ import annotations

import json

from cyclopts import App
from nornir.core.exceptions import OperationInputError

from nornir_sandbox import init_nornir_sync
from nornir_sandbox.cli import AsJson, Hosts, OperationParams, SyncRunner, describe_result, format_interfaces
from nornir_sandbox.operations import GetInterfaces

app = App()


@app.default
def main(
    *, hosts: Hosts = None, runner: SyncRunner = "threaded", params: OperationParams = "{}", as_json: AsJson = False
) -> int:
    """Print the administrative and operational state, MTU and description of the
    interfaces of each device."""
    with init_nornir_sync(hosts=hosts, runner=runner) as nr:
        try:
            run = nr.run_operation(GetInterfaces, json.loads(params))
        except OperationInputError as error:
            print(f"invalid --params: {error}")
            return 2
        print(f"{'host':<8} {'interface':<15} {'adm/oper':<9} {'mtu':>6}  description")
        with run as stream:
            for res in stream.results():
                if res.failed:
                    print(f"{res.host!s:<8} FAILED   {describe_result(res)}")
                elif as_json:
                    print(f"{res.host}: {res.result.model_dump_json()}")
                else:
                    print(format_interfaces(str(res.host), res.result))
            result = stream.result()
    return int(result.failed)


if __name__ == "__main__":
    app()
