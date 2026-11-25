"""Data sent between the driver and the Ray actors."""

from __future__ import annotations

import pickle
import threading
from typing import Any

import pytest
from nornir.core.task import PartialResult, Result
from ray import cloudpickle

from nornir_ray.errors import RemoteTaskError, TaskNotSerializableError
from nornir_ray.wire import (
    TaskCall,
    WirePartial,
    detach_host,
    ensure_serializable,
    from_wire_partial,
    from_wire_result,
    to_wire_partial,
    to_wire_result,
)
from nornir_sandbox import init_nornir_sync


def _call(fn: Any, **params: Any) -> TaskCall:
    nr = init_nornir_sync(runner="threaded")
    return TaskCall(fn=fn, params=params, name="my_task", severity_level=20, global_dry_run=False, config=nr.config)


def test_detach_host_drops_connections() -> None:
    host = init_nornir_sync(runner="threaded").inventory.hosts["ceos1"]
    host.connections["scrapli"] = threading.Lock()  # type: ignore[assignment]  # not picklable

    detached = detach_host(host)

    assert detached.connections == {}
    assert "scrapli" in host.connections
    assert pickle.loads(cloudpickle.dumps(detached)).hostname == host.hostname


def test_detached_host_keeps_group_and_default_values() -> None:
    host = init_nornir_sync(runner="threaded").inventory.hosts["ceos1"]
    restored = pickle.loads(cloudpickle.dumps(detach_host(host)))
    assert restored.platform == "arista_eos"
    assert restored.name == "ceos1"


def test_config_round_trips() -> None:
    config = init_nornir_sync(runner="serial").config
    assert pickle.loads(cloudpickle.dumps(config)).runner.plugin == "serial"


def test_ensure_serializable_rejects_unpicklable_function() -> None:
    lock = threading.Lock()

    def uses_lock(task: Any) -> None:
        with lock:
            pass

    with pytest.raises(TaskNotSerializableError, match="my_task"):
        ensure_serializable(_call(uses_lock))


def test_ensure_serializable_rejects_unpicklable_params() -> None:
    def plain(task: Any, lock: Any) -> None:
        pass

    with pytest.raises(TaskNotSerializableError, match="my_task"):
        ensure_serializable(_call(plain, lock=threading.Lock()))


def test_ensure_serializable_accepts_a_plain_task() -> None:
    def plain(task: Any, value: int) -> int:
        return value

    ensure_serializable(_call(plain, value=1))


def test_result_round_trip_keeps_fields_and_extra_attributes() -> None:
    host = init_nornir_sync(runner="threaded").inventory.hosts["ceos1"]
    result = Result(host, result="tb", changed=True, diff="d", failed=True, exception=ValueError("boom"))
    result.name = "my_task"

    wire = pickle.loads(pickle.dumps(to_wire_result(result)))
    back = from_wire_result(wire, host, ray_node="n1", ray_site="site_a")

    assert (back.name, back.result, back.changed, back.diff, back.failed) == ("my_task", "tb", True, "d", True)
    assert isinstance(back.exception, ValueError)
    assert str(back.exception) == "boom"
    assert back.host is host
    assert getattr(back, "ray_node", None) == "n1"
    assert getattr(back, "ray_site", None) == "site_a"


class _UnpicklableError(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.lock = threading.Lock()


def test_unpicklable_exception_becomes_remote_task_error() -> None:
    result = Result(None, failed=True, exception=_UnpicklableError("bad state"))

    wire = pickle.loads(cloudpickle.dumps(to_wire_result(result)))

    assert isinstance(wire.exception, RemoteTaskError)
    assert "_UnpicklableError" in str(wire.exception)
    assert "bad state" in str(wire.exception)


def test_partial_round_trip() -> None:
    host = init_nornir_sync(runner="threaded").inventory.hosts["ceos1"]

    wire = pickle.loads(pickle.dumps(to_wire_partial(PartialResult(host, "half", progress=0.5))))

    assert wire == WirePartial(result="half", progress=0.5)
    back = from_wire_partial(wire, host)
    assert (back.host, back.result, back.progress) == (host, "half", 0.5)


class _TwoArgumentError(Exception):
    """Like Nornir's own errors: its constructor takes arguments that `args` does not hold."""

    def __init__(self, connection: str, mode: str) -> None:
        super().__init__(f"{connection} is open in {mode} mode")
        self.connection = connection
        self.mode = mode


def test_exception_with_required_arguments_keeps_its_type() -> None:
    result = Result(None, failed=True, exception=_TwoArgumentError("fake", "sync"))

    back = from_wire_result(pickle.loads(cloudpickle.dumps(to_wire_result(result))), None)  # type: ignore[arg-type]

    assert isinstance(back.exception, _TwoArgumentError)
    assert (back.exception.connection, back.exception.mode) == ("fake", "sync")
    assert str(back.exception) == "fake is open in sync mode"
