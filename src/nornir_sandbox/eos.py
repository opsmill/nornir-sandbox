"""Helpers to run commands on Arista EOS over the host's scrapli connection.

`send` and `send_json` are coroutines for tasks on the asyncio runner. `send_sync`
and `send_json_sync` are their counterparts for tasks on the threaded and serial
runners. The `scrapli` connection supports both modes; Nornir opens it in the mode
of the runner that runs the task.

`configure` and `configure_sync` apply configuration lines in configuration mode.
"""

from __future__ import annotations

import json
from typing import Any

from nornir.core.task import Task

PLATFORM = "arista_eos"
"""Nornir platform of the cEOS hosts in inventory/groups.yaml."""


class EOSCommandError(Exception):
    """EOS rejected a command."""

    def __init__(self, command: str, output: str) -> None:
        super().__init__(f"EOS rejected {command!r}: {output.strip()}")
        self.command = command
        self.output = output


async def send(task: Task, command: str) -> str:
    """Run one command and return its text output. Raises EOSCommandError if EOS rejects it."""
    conn = await task.host.aget_connection("scrapli", task.nornir.config)
    response = await conn.send_command(command)
    return _output(command, response)


async def send_json(task: Task, command: str) -> dict[str, Any]:
    """Run a show command with `| json` and return the decoded output."""
    return _decode(command, await send(task, f"{command} | json"))


def send_sync(task: Task, command: str) -> str:
    """Run one command and return its text output. Raises EOSCommandError if EOS rejects it."""
    conn = task.host.get_connection("scrapli", task.nornir.config)
    response = conn.send_command(command)
    return _output(command, response)


def send_json_sync(task: Task, command: str) -> dict[str, Any]:
    """Run a show command with `| json` and return the decoded output."""
    return _decode(command, send_sync(task, f"{command} | json"))


async def configure(task: Task, lines: list[str]) -> None:
    """Apply configuration lines in configuration mode. Raises EOSCommandError if EOS rejects one."""
    conn = await task.host.aget_connection("scrapli", task.nornir.config)
    _check_configs(lines, await conn.send_configs(lines))


def configure_sync(task: Task, lines: list[str]) -> None:
    """Apply configuration lines in configuration mode. Raises EOSCommandError if EOS rejects one."""
    conn = task.host.get_connection("scrapli", task.nornir.config)
    _check_configs(lines, conn.send_configs(lines))


def sleep_command(seconds: float) -> str:
    """Return an EOS command that returns after `seconds`, run by the device's Linux shell.

    The runner examples use it to make a host slow on the device side.
    """
    return f"bash timeout {int(seconds) + 30} sleep {seconds:g}"


def _output(command: str, response: Any) -> str:
    if response.failed:
        raise EOSCommandError(command, str(response.result))
    return str(response.result)


def _check_configs(lines: list[str], responses: Any) -> None:
    for response in responses:
        if response.failed:
            raise EOSCommandError(response.channel_input, str(response.result))
    if responses.failed:
        raise EOSCommandError("; ".join(lines), "configuration failed")


def _decode(command: str, output: str) -> dict[str, Any]:
    data = json.loads(output)
    if not isinstance(data, dict):
        raise TypeError(f"{command!r} returned {type(data).__name__}, expected a JSON object")
    return data
