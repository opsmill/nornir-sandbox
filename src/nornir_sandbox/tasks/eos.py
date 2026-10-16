"""Helpers to run commands on Arista EOS over the host's asyncio scrapli connection."""

from __future__ import annotations

import json
from typing import Any

from nornir.core.task import Task


async def send(task: Task, command: str) -> str:
    """Run one command and return its text output. Raises if EOS reports an error."""
    conn = await task.host.aget_connection("scrapli", task.nornir.config)
    response = await conn.send_command(command)
    response.raise_for_status()
    return str(response.result)


async def send_json(task: Task, command: str) -> dict[str, Any]:
    """Run a show command with `| json` and return the decoded output."""
    data = json.loads(await send(task, f"{command} | json"))
    if not isinstance(data, dict):
        raise TypeError(f"{command!r} returned {type(data).__name__}, expected a JSON object")
    return data
