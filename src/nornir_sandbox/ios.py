from __future__ import annotations

from typing import Any


from nornir.core.task import Task

PLATFROM="cisco_ios"

class IOSCommandError(Exception):
    """IOS rejected a command"""

    def __init__(self, command: str, output: str) -> None:
        super().__init__(f"IOS rejected {command!r}: {output.strip()}")
        self.command = command
        self.output = output


async def send(task: Task, command: str) -> str:
    """Run one command and return its text output. Raises IOSCommandError if EOS rejects it."""

    conn = await task.host.aget_connection("scrapli", task.nornir.config)
    response = await conn.send_command(command)
    return _output(command, response)

def send_sync(task: Task, command: str) -> str:
    """Run one command and return its text output. Raises IOSCommandError if EOS rejects it."""

    conn = task.host.get_connection("scrapli", task.nornir.config)
    response = conn.send_command(command)
    return _output(command, response)

def send_and_parse_sync(task: Task, command: str) -> dict[str, Any] | list[Any]:
    """Run one command and return its parsed output. Raises IOSCommandError if EOS rejects it."""

    conn = task.host.get_connection("scrapli", task.nornir.config)
    response = conn.send_command(command)
    return _output_parsed(command, response)

async def send_and_parse(task: Task, command: str) -> dict[str, Any] | list[Any]:
    """Run one command and return its parsed output. Raises IOSCommandError if EOS rejects it."""

    conn = await task.host.aget_connection("scrapli", task.nornir.config)
    response = await conn.send_command(command)
    return _output_parsed(command, response)


def _output_parsed(command: str, response: Any) -> dict[str, Any] | list[Any]:
    if response.failed:
        raise IOSCommandError(command, str(response.result))
    return response.textfsm_parse_output()


def _output(command: str, response: Any) -> str:
    if response.failed:
        raise IOSCommandError(command, str(response.result))
    return str(response.result)
