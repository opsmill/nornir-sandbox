"""Connecting to a Ray cluster."""

from __future__ import annotations

from typing import Any

import pytest
import ray

from nornir_ray.cluster import connect, local_ip_towards


def test_local_ip_towards_loopback_is_loopback() -> None:
    assert local_ip_towards("127.0.0.1:6379") == "127.0.0.1"


def test_connect_without_address_starts_a_new_local_ray(monkeypatch: pytest.MonkeyPatch) -> None:
    # A Ray cluster this machine has joined (`invoke ray-up`) must not be picked up.
    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(ray, "is_initialized", lambda: False)
    monkeypatch.setattr(ray, "init", lambda **kwargs: calls.append(kwargs))

    connect(None, {"site_a": 1.0})

    assert calls == [{"address": "local", "resources": {"site_a": 1.0}}]
