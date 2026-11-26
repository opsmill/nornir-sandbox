"""Backup directories under backups/, shared by the backup scripts."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from nornir_sandbox.paths import BACKUP_DIR


def new_backup_dir() -> Path:
    """Return backups/<YYYYmmdd-HHMMSS> for a new run; the directory is not created."""
    return BACKUP_DIR / datetime.now().strftime("%Y%m%d-%H%M%S")


def write_config(path: Path, config: str) -> None:
    """Write `config` to `path`, creating the directory, with a final newline. Blocks."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(config if config.endswith("\n") else config + "\n")


def link_latest(target: Path) -> None:
    """Point backups/latest at `target`, a directory under backups/."""
    latest = BACKUP_DIR / "latest"
    latest.unlink(missing_ok=True)
    latest.symlink_to(target.name)
    print(f"\nbackups/latest -> {target.name}")
