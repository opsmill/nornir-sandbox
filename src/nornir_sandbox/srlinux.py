"""Helpers to run commands on Nokia SR Linux over the host's scrapli connection.

The counterpart of eos.py for the srl1 host. `send`, `send_json` and `configure` are
coroutines for tasks on the asyncio runner; the `*_sync` functions are for tasks on
the threaded and serial runners.

scrapli does not treat the SR Linux "Parsing error" and "Error:" messages as failures,
so these helpers pass them in `failed_when_contains`.
"""

from __future__ import annotations

import json
from typing import Any

from nornir.core.task import Task

PLATFORM = "nokia_srlinux"
"""Nornir platform of the SR Linux hosts in inventory/groups.yaml."""

FAILED_WHEN_CONTAINS = ["Parsing error", "Error:", "MINOR:", "MAJOR:", "CRITICAL:"]

# `commit stay` commits and stays in candidate mode. `commit now` leaves candidate mode
# without scrapli knowing, so the next configure() would send `set` in running mode.
COMMIT = "commit stay"


class SRLinuxCommandError(Exception):
    """SR Linux rejected a command."""

    def __init__(self, command: str, output: str) -> None:
        super().__init__(f"SR Linux rejected {command!r}: {output.strip()}")
        self.command = command
        self.output = output


async def send(task: Task, command: str) -> str:
    """Run one command and return its text output. Raises SRLinuxCommandError if SR Linux rejects it."""
    conn = await task.host.aget_connection("scrapli", task.nornir.config)
    response = await conn.send_command(command, failed_when_contains=FAILED_WHEN_CONTAINS)
    return _output(command, response)


async def send_json(task: Task, command: str) -> dict[str, Any]:
    """Run an `info` or `show` command with `| as json` and return the decoded output."""
    return _decode(command, await send(task, f"{command} | as json"))


async def configure(task: Task, lines: list[str]) -> None:
    """Apply `set` lines in candidate mode and commit them. Raises SRLinuxCommandError on failure."""
    conn = await task.host.aget_connection("scrapli", task.nornir.config)
    _check_configs(await conn.send_configs([*lines, COMMIT], failed_when_contains=FAILED_WHEN_CONTAINS))


def send_sync(task: Task, command: str) -> str:
    """Run one command and return its text output. Raises SRLinuxCommandError if SR Linux rejects it."""
    conn = task.host.get_connection("scrapli", task.nornir.config)
    response = conn.send_command(command, failed_when_contains=FAILED_WHEN_CONTAINS)
    return _output(command, response)


def send_json_sync(task: Task, command: str) -> dict[str, Any]:
    """Run an `info` or `show` command with `| as json` and return the decoded output."""
    return _decode(command, send_sync(task, f"{command} | as json"))


def configure_sync(task: Task, lines: list[str]) -> None:
    """Apply `set` lines in candidate mode and commit them. Raises SRLinuxCommandError on failure."""
    conn = task.host.get_connection("scrapli", task.nornir.config)
    _check_configs(conn.send_configs([*lines, COMMIT], failed_when_contains=FAILED_WHEN_CONTAINS))


def _output(command: str, response: Any) -> str:
    if response.failed:
        raise SRLinuxCommandError(command, str(response.result))
    return str(response.result)


def _check_configs(responses: Any) -> None:
    for response in responses:
        if response.failed:
            raise SRLinuxCommandError(response.channel_input, str(response.result))


def _decode(command: str, output: str) -> dict[str, Any]:
    data = json.loads(output)
    if not isinstance(data, dict):
        raise TypeError(f"{command!r} returned {type(data).__name__}, expected a JSON object")
    return data
