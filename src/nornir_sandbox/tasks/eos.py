"""Helpers to run commands on Arista EOS over the host's scrapli connection."""

from __future__ import annotations

import json
from typing import Any

from nornir.core.task import Task
from scrapli import Cli


class CommandError(Exception):
    """EOS answered a command with one of the failure messages of the scrapli arista_eos definition."""


def send(task: Task, command: str) -> str:
    """Run one command and return its text output. Raises CommandError if EOS reports an error."""
    conn: Cli = task.host.get_connection("scrapli", task.nornir.config)
    response = conn.send_input(command)
    if response.failed:
        raise CommandError(f"{command!r}: {response.result}")
    return response.result


def send_json(task: Task, command: str) -> dict[str, Any]:
    """Run a show command with `| json` and return the decoded output."""
    data = json.loads(send(task, f"{command} | json"))
    if not isinstance(data, dict):
        raise TypeError(f"{command!r} returned {type(data).__name__}, expected a JSON object")
    return data
