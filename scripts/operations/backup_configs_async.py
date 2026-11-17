"""Back up the running configuration of every host with the get_config Operation, on
the asyncio runner.

scripts/backup/backup_configs.py does the same on the cEOS hosts with a plain task that
sends `show running-config`. Here the Operation selects the command for each platform,
and the script writes the files as the results arrive.

Unlike the other Operations, get_config has no task entry points in pyproject.toml:
register_tasks() registers its `async def` tasks with OperationRegistry.register()
before the script runs it. A program can register its own tasks this way, without
packaging them.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

from cyclopts import App
from nornir.core.operations import OperationRegistry

from nornir_sandbox import init_nornir
from nornir_sandbox.backups import link_latest, new_backup_dir, write_config
from nornir_sandbox.cli import Hosts, describe_result
from nornir_sandbox.operations import ASYNC_CONNECTION, GetConfig, eos, srlinux

app = App()


def register_tasks() -> None:
    """Register the `async def` get_config task of each platform under the connection
    name that init_nornir() sets for the asyncio runner."""
    for platform, task in (("arista_eos", eos.get_config), ("nokia_srlinux", srlinux.get_config)):
        OperationRegistry.register(operation=GetConfig.name, platform=platform, connection=ASYNC_CONNECTION, task=task)


@app.default
async def main(*, hosts: Hosts = None, output_dir: Path | None = None) -> int:
    """Save the running configuration of each device to <output_dir>/<host>.cfg.

    Parameters
    ----------
    output_dir
        Directory for the .cfg files. Default: backups/<YYYYmmdd-HHMMSS>, and
        backups/latest points at it.
    """
    register_tasks()
    target = output_dir or new_backup_dir()
    nr = await init_nornir(hosts=hosts)
    async with nr, nr.arun_operation(GetConfig) as stream:
        async for res in stream.results():
            if res.failed:
                print(f"{res.host!s:<8} FAILED  {describe_result(res)}")
                continue
            path = target / f"{res.host}.cfg"
            # File writes block; run them in a thread so the other hosts keep running.
            await asyncio.to_thread(write_config, path, res.result.config)
            print(f"{res.host!s:<8} OK      saved {len(res.result.config.splitlines())} lines to {path}")
        result = await stream.result()
    if output_dir is None and target.exists():
        link_latest(target)
    return int(result.failed)


if __name__ == "__main__":
    app()
