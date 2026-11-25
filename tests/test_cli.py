"""Output helpers shared by the scripts."""

from __future__ import annotations

from nornir.core.task import Result

from nornir_sandbox.cli import format_event


def test_format_event_names_the_node() -> None:
    result = Result(None, result="done", ray_node="ray-site-a")
    assert format_event(result, 0.0).endswith("[node ray-site-a]")


def test_format_event_without_node_is_unchanged() -> None:
    assert "[node" not in format_event(Result(None, result="done"), 0.0)
