from __future__ import annotations

from cyclopts import App
from nornir_scrapli.tasks import send_commands
from nornir_utils.plugins.functions import print_result

from nornir_sandbox import init_nornir
from nornir_sandbox.cli import Hosts

app = App()


@app.default
def main(commands: list[str], /, *, hosts: Hosts = None) -> int:
    """Run show commands on the lab devices and print the output.

    Example: run_commands.py "show version" "show lldp neighbors" --hosts ceos1

    Parameters
    ----------
    commands
        Commands to run, in order.
    """
    result = init_nornir(hosts=hosts).run(task=send_commands, commands=commands)
    print_result(result)
    return int(result.failed)


if __name__ == "__main__":
    app()
