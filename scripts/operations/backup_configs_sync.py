"""Back up the running configuration of every host with the get_config Operation, on
the threaded or serial runner.

The sync counterpart of backup_configs_async.py.
"""

from __future__ import annotations

from pathlib import Path

from cyclopts import App

from nornir_sandbox import init_nornir_sync
from nornir_sandbox.backups import link_latest, new_backup_dir, write_config
from nornir_sandbox.cli import Hosts, SyncRunner, describe_result
from nornir_sandbox.operations import GetConfig

app = App()


@app.default
def main(*, hosts: Hosts = None, output_dir: Path | None = None, runner: SyncRunner = "threaded") -> int:
    """Save the running configuration of each device to <output_dir>/<host>.cfg.

    Parameters
    ----------
    output_dir
        Directory for the .cfg files. Default: backups/<YYYYmmdd-HHMMSS>, and
        backups/latest points at it.
    """
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
