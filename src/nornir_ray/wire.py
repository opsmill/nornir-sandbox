"""Plain, picklable data sent between the driver and the Ray actors.

Nornir objects hold references to the whole Nornir object, its processors and live
connections, none of which can travel to another process. The driver sends a
`TaskCall` and a detached copy of each host; the actor sends back `WirePartial` and
`WireFinal` events, which the driver turns into Nornir results for its own hosts.
"""

from __future__ import annotations

import copy
import pickle
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from nornir.core.configuration import Config
from nornir.core.inventory import Host
from nornir.core.task import PartialResult, Result
from ray import cloudpickle

from nornir_ray.errors import RemoteTaskError, TaskNotSerializableError


@dataclass(frozen=True)
class TaskCall:
    """Everything an actor needs to run one task for its host."""

    fn: Callable[..., Any]
    params: dict[str, Any]
    name: str
    severity_level: int
    global_dry_run: bool
    config: Config


@dataclass(frozen=True)
class WireResult:
    """One `Result` without its host."""

    name: str | None
    result: Any
    changed: bool
    diff: str
    failed: bool
    exception: BaseException | None
    severity_level: int


@dataclass(frozen=True)
class WirePartial:
    """One `PartialResult` of a top-level task, without its host."""

    result: Any
    progress: float | None


@dataclass(frozen=True)
class WireFinal:
    """The results of one host: `results[0]` is the task's result, the others its subtasks'."""

    results: list[WireResult]
    node: str
    site: str | None


WireEvent = WirePartial | WireFinal


def detach_host(host: Host) -> Host:
    """Return a copy of `host` with no connections, which can be pickled.

    The copy shares the host's groups, defaults and data; only the connection state
    is new and empty, so the driver's open connections never travel to a worker.
    """
    detached = copy.copy(host)
    detached.connections = {}
    detached._async_connections = {}
    detached._capability_connections = {}
    detached._opening = {}
    return detached


def ensure_serializable(call: TaskCall) -> None:
    """Raise `TaskNotSerializableError` if the task function or its parameters cannot be pickled."""
    try:
        cloudpickle.dumps(call.fn)
        cloudpickle.dumps(call.params)
    except Exception as error:
        raise TaskNotSerializableError(call.name, str(error)) from error


def _portable_exception(exception: BaseException | None) -> BaseException | None:
    if exception is None:
        return None
    try:
        pickle.loads(pickle.dumps(exception))
    except Exception:
        return RemoteTaskError(type(exception).__name__, str(exception))
    return exception


def to_wire_result(result: Result) -> WireResult:
    """Return `result` without its host. An exception that cannot be pickled becomes `RemoteTaskError`."""
    return WireResult(
        name=result.name,
        result=result.result,
        changed=result.changed,
        diff=result.diff,
        failed=result.failed,
        exception=_portable_exception(result.exception),
        severity_level=result.severity_level,
    )


def from_wire_result(wire: WireResult, host: Host, **extra: Any) -> Result:
    """Return a `Result` for `host`. Each `extra` keyword becomes an attribute of the result."""
    result = Result(
        host,
        result=wire.result,
        changed=wire.changed,
        diff=wire.diff,
        failed=wire.failed,
        exception=wire.exception,
        severity_level=wire.severity_level,
        **extra,
    )
    result.name = wire.name
    return result


def to_wire_partial(partial: PartialResult) -> WirePartial:
    """Return `partial` without its host."""
    return WirePartial(result=partial.result, progress=partial.progress)


def from_wire_partial(wire: WirePartial, host: Host) -> PartialResult:
    """Return a `PartialResult` for `host`."""
    return PartialResult(host, wire.result, progress=wire.progress)
