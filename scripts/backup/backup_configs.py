from __future__ import annotations

from datetime import datetime
from pathlib import Path

from cyclopts import App

from nornir_sandbox import init_nornir
from nornir_sandbox.cli import Hosts, Verbose, print_summary
from nornir_sandbox.paths import BACKUP_DIR
from nornir_sandbox.tasks import backup_config

app = App()


@app.default
def main(*, hosts: Hosts = None, output_dir: Path | None = None, verbose: Verbose = False) -> int:
    """Back up the running configuration of the lab devices.

    Each run writes to backups/<YYYYmmdd-HHMMSS>/<host>.cfg and points
    backups/latest at it. Compare two runs with:
    diff -r backups/<older run> backups/latest

    Parameters
    ----------
    output_dir
        Directory for the .cfg files. Default: backups/<YYYYmmdd-HHMMSS>.
    """
    target = output_dir or BACKUP_DIR / datetime.now().strftime("%Y%m%d-%H%M%S")
    nr = init_nornir(hosts=hosts)
    exit_code = print_summary([nr.run(task=backup_config, backup_dir=target)], verbose=verbose)

    if output_dir is None and target.exists():
        latest = BACKUP_DIR / "latest"
        latest.unlink(missing_ok=True)
        latest.symlink_to(target.name)
        print(f"\nbackups/latest -> {target.name}")
    return exit_code


if __name__ == "__main__":
    app()
