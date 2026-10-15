from __future__ import annotations

import socket

from nornir.core.task import Result, Task

from nornir_sandbox.tasks.eos import send_json


def check_access(task: Task, port: int = 22, timeout: float = 3.0) -> Result:
    """Check, in order, that the SSH port answers, that login works and that EOS runs commands.

    Failures are returned as a failed Result with the step that failed, instead
    of a traceback, so the output says where to look.
    """
    hostname = task.host.hostname or task.host.name
    try:
        with socket.create_connection((hostname, port), timeout=timeout):
            pass
    except OSError as exc:
        return Result(host=task.host, failed=True, result=f"{hostname}:{port} not reachable: {exc}")

    try:
        version = send_json(task, "show version")
    except Exception as exc:
        # A failed login leaves no connection behind, so the next attempt starts clean.
        return Result(host=task.host, failed=True, result=f"SSH login or command failed: {exc}")

    uptime = int(float(version.get("uptime", 0)))
    return Result(
        host=task.host,
        result=f"{version.get('modelName')} EOS {version.get('version')}, up {uptime}s",
    )
