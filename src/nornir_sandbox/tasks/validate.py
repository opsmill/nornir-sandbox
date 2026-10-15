from __future__ import annotations

from nornir.core.task import Result, Task

from nornir_sandbox.checks import Link, compare_interfaces, compare_lldp
from nornir_sandbox.tasks.eos import send_json


def _links(task: Task) -> list[Link]:
    return [Link(**link) for link in task.host.get("links", [])]


def _result(task: Task, problems: list[str], ok_message: str) -> Result:
    if problems:
        return Result(host=task.host, failed=True, result="; ".join(problems))
    return Result(host=task.host, result=ok_message)


def validate_interfaces(task: Task) -> Result:
    """Every interface listed in the host's links exists and has line protocol up."""
    expected = [link.interface for link in _links(task)]
    interfaces = send_json(task, "show interfaces").get("interfaces", {})
    return _result(task, compare_interfaces(expected, interfaces), f"{len(expected)} link interfaces up")


def validate_lldp(task: Task) -> Result:
    """LLDP neighbors match the host's links, with no extra neighbors."""
    expected = _links(task)
    neighbors = send_json(task, "show lldp neighbors").get("lldpNeighbors", [])
    return _result(task, compare_lldp(expected, neighbors), f"{len(expected)} LLDP neighbors match the inventory")
