"""Locations inside the repository.

The package is installed in editable mode by `uv sync`, so the repository root is
two levels above this file (src/nornir_sandbox/paths.py).
"""

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
INVENTORY_DIR = REPO_ROOT / "inventory"
BACKUP_DIR = REPO_ROOT / "backups"
LOG_FILE = REPO_ROOT / "nornir.log"
