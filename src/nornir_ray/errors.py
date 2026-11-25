"""Errors raised by the Ray runner."""

from __future__ import annotations


class TaskNotSerializableError(Exception):
    """The task or its parameters cannot be sent to the Ray workers."""

    def __init__(self, task_name: str, reason: str) -> None:
        super().__init__(f"task {task_name!r} cannot be sent to Ray workers: {reason}")
        self.task_name = task_name
        self.reason = reason

    def __reduce__(self) -> tuple[type[TaskNotSerializableError], tuple[str, str]]:
        return type(self), (self.task_name, self.reason)


class RayClusterUnreachableError(Exception):
    """The Ray cluster address does not answer."""

    def __init__(self, address: str, reason: str) -> None:
        super().__init__(f"cannot reach the Ray cluster at {address}: {reason}")
        self.address = address
        self.reason = reason

    def __reduce__(self) -> tuple[type[RayClusterUnreachableError], tuple[str, str]]:
        return type(self), (self.address, self.reason)


class HostActorDiedError(Exception):
    """The Ray actor running a host's task stopped during the run."""

    def __init__(self, host: str, node: str | None, site: str | None, reason: str) -> None:
        super().__init__(
            f"the Ray actor of host {host!r} (node {node or 'unknown'}, site {site or 'none'}) died: {reason}"
        )
        self.host = host
        self.node = node
        self.site = site
        self.reason = reason

    def __reduce__(self) -> tuple[type[HostActorDiedError], tuple[str, str | None, str | None, str]]:
        return type(self), (self.host, self.node, self.site, self.reason)


class RemoteTaskError(Exception):
    """Stands in for an exception raised on a worker that cannot be sent back to the driver."""

    def __init__(self, type_name: str, message: str) -> None:
        super().__init__(f"{type_name}: {message}")
        self.type_name = type_name
        self.message = message

    def __reduce__(self) -> tuple[type[RemoteTaskError], tuple[str, str]]:
        return type(self), (self.type_name, self.message)
