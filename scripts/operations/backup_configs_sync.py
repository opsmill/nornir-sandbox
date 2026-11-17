"""Back up the running configuration of every host with the get_config Operation, on
the threaded or serial runner.

The sync counterpart of backup_configs_async.py. register_tasks() registers the `def`
tasks of get_config with OperationRegistry.register() before the script runs it, as
get_config has no task entry points in pyproject.toml.
"""

from __future__ import annotations

from pathlib import Path

from cyclopts import App
from nornir.core.operations import OperationRegistry

from nornir_sandbox import init_nornir_sync
from nornir_sandbox.backups import link_latest, new_backup_dir, write_config
from nornir_sandbox.cli import Hosts, SyncRunner, describe_result
from nornir_sandbox.operations import SYNC_CONNECTION, GetConfig, eos, srlinux

app = App()


def register_tasks() -> None:
    """Register the `def` get_config task of each platform under the connection name
    that init_nornir_sync() sets for the threaded and serial runners."""
    for platform, task in (("arista_eos", eos.get_config_sync), ("nokia_srlinux", srlinux.get_config_sync)):
        OperationRegistry.register(operation=GetConfig.name, platform=platform, connection=SYNC_CONNECTION, task=task)


@app.default
def main(*, hosts: Hosts = None, output_dir: Path | None = None, runner: SyncRunner = "threaded") -> int:
    """Save the running configuration of each device to <output_dir>/<host>.cfg.

    Parameters
    ----------
    output_dir
        Directory for the .cfg files. Default: backups/<YYYYmmdd-HHMMSS>, and
        backups/latest points at it.
    """
    register_tasks()
    target = output_dir or new_backup_dir()
    with init_nornir_sync(hosts=hosts, runner=runner) as nr, nr.run_operation(GetConfig) as stream:
        for res in stream.results():
            if res.failed:
                print(f"{res.host!s:<8} FAILED  {describe_result(res)}")
                continue
            path = target / f"{res.host}.cfg"
            write_config(path, res.result.config)
            print(f"{res.host!s:<8} OK      saved {len(res.result.config.splitlines())} lines to {path}")
        result = stream.result()
    if output_dir is None and target.exists():
        link_latest(target)
    return int(result.failed)


if __name__ == "__main__":
    app()
