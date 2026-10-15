"""Nornir tasks. Each task returns a Result whose `result` is a one-line summary or the collected data."""

from nornir_sandbox.tasks.backup import backup_config
from nornir_sandbox.tasks.troubleshoot import check_access
from nornir_sandbox.tasks.validate import validate_interfaces, validate_lldp

__all__ = ["backup_config", "check_access", "validate_interfaces", "validate_lldp"]
