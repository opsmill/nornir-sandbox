from __future__ import annotations

from pathlib import Path

from nornir.core.task import Result, Task

from nornir_sandbox.tasks.eos import send


def backup_config(task: Task, backup_dir: Path) -> Result:
    """Save the running configuration to <backup_dir>/<host>.cfg."""
    config = send(task, "show running-config")
    backup_dir.mkdir(parents=True, exist_ok=True)
    path = backup_dir / f"{task.host.name}.cfg"
    path.write_text(config if config.endswith("\n") else config + "\n")
    return Result(host=task.host, result=f"saved {len(config.splitlines())} lines to {path}")
