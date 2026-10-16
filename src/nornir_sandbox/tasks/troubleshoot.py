from __future__ import annotations

import asyncio

from nornir.core.task import Result, Task

from nornir_sandbox.tasks.eos import send_json


async def check_access(task: Task, port: int = 22, timeout: float = 3.0) -> Result:
    """Check, in order, that the SSH port answers, that login works and that EOS runs commands.

    Failures are returned as a failed Result with the step that failed, instead
    of a traceback, so the output says where to look.
    """
    hostname = task.host.hostname or task.host.name
    try:
        _, writer = await asyncio.wait_for(asyncio.open_connection(hostname, port), timeout=timeout)
        writer.close()
        await writer.wait_closed()
    except (OSError, TimeoutError) as exc:
        reason = str(exc) or f"no answer after {timeout}s"
        return Result(host=task.host, failed=True, result=f"{hostname}:{port} not reachable: {reason}")

    try:
        version = await send_json(task, "show version")
    except Exception as exc:
        # A failed login leaves no connection behind, so the next attempt starts clean.
        return Result(host=task.host, failed=True, result=f"SSH login or command failed: {exc}")

    uptime = int(float(version.get("uptime", 0)))
    return Result(
        host=task.host,
        result=f"{version.get('modelName')} EOS {version.get('version')}, up {uptime}s",
    )
