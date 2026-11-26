"""Run the get_interfaces Operation, whose models are Pydantic models, with
nr.arun_operation() on the asyncio runner.

--params takes the input of the Operation as JSON, for example '{"oper_state": "up"}'.
The script passes the decoded mapping to arun_operation() as it is: Nornir validates it
with GetInterfacesInput.model_validate(), so a misspelled field or a state other than
"up" or "down" raises OperationInputError before any host connects. --json prints each
result with model_dump_json().
"""

from __future__ import annotations

import json

from cyclopts import App
from nornir.core.exceptions import OperationInputError

from nornir_sandbox import init_nornir
from nornir_sandbox.cli import AsJson, Hosts, OperationParams, describe_result, format_interfaces
from nornir_sandbox.operations import GetInterfaces

app = App()


@app.default
async def main(*, hosts: Hosts = None, params: OperationParams = "{}", as_json: AsJson = False) -> int:
    """Print the administrative and operational state, MTU and description of the
    interfaces of each device."""
    nr = await init_nornir(hosts=hosts)
    async with nr:
        try:
            run = nr.arun_operation(GetInterfaces, json.loads(params))
        except OperationInputError as error:
            print(f"invalid --params: {error}")
            return 2
        print(f"{'host':<8} {'interface':<15} {'adm/oper':<9} {'mtu':>6}  description")
        async with run as stream:
            async for res in stream.results():
                if res.failed:
                    print(f"{res.host!s:<8} FAILED   {describe_result(res)}")
                elif as_json:
                    print(f"{res.host}: {res.result.model_dump_json()}")
                else:
                    print(format_interfaces(str(res.host), res.result))
            result = await stream.result()
    return int(result.failed)


if __name__ == "__main__":
    app()
