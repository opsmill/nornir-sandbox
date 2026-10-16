from __future__ import annotations

import asyncio
from pathlib import Path

from nornir.core.task import Result, Task

from nornir_sandbox.tasks.eos import send


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
