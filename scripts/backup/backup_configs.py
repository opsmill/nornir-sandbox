from __future__ import annotations

import asyncio
from datetime import datetime
from pathlib import Path

from cyclopts import App
from nornir.core.task import Result, Task

from nornir_sandbox import init_nornir
from nornir_sandbox.cli import Hosts, Verbose, print_summary
from nornir_sandbox.eos import send
from nornir_sandbox.paths import BACKUP_DIR

app = App()


def _write_config(path: Path, config: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(config if config.endswith("\n") else config + "\n")


async def backup_config(task: Task, backup_dir: Path) -> Result:
    """Save the running configuration to <backup_dir>/<host>.cfg."""
    config = await send(task, "show running-config")
    path = backup_dir / f"{task.host.name}.cfg"
    # File writes block; run them in a thread so the other hosts keep running.
    await asyncio.to_thread(_write_config, path, config)
    return Result(host=task.host, result=f"saved {len(config.splitlines())} lines to {path}")


@app.default
async def main(*, hosts: Hosts = None, output_dir: Path | None = None, verbose: Verbose = False) -> int:
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
    nr = await init_nornir(hosts=hosts)
    async with nr:
        result = await nr.arun(task=backup_config, backup_dir=target)
    exit_code = print_summary([result], verbose=verbose)

    if output_dir is None and target.exists():
        latest = BACKUP_DIR / "latest"
        latest.unlink(missing_ok=True)
        latest.symlink_to(target.name)
        print(f"\nbackups/latest -> {target.name}")
    return exit_code


if __name__ == "__main__":
    app()
