"""Shared test setup. The `ray_local` fixture starts Ray only for the tests that use it."""

from __future__ import annotations

import os
from collections.abc import Iterator

import pytest

# Ray reads this when it is first imported; see src/nornir_ray/__init__.py. It is set
# here, before any import of Ray, so it does not depend on the order of imports.
os.environ.setdefault("RAY_ENABLE_UV_RUN_RUNTIME_ENV", "0")

import ray

# Each lab site is a Ray custom resource; the local Ray declares both, so actors placed by
# site can run without the lab's Ray containers.
LOCAL_SITES = {"site_a": 100.0, "site_b": 100.0}


@pytest.fixture(scope="session")
def ray_local() -> Iterator[None]:
    """Start a local single-node Ray for the whole test session."""
    started = False
    if not ray.is_initialized():
        ray.init(num_cpus=4, resources=LOCAL_SITES, include_dashboard=False, log_to_driver=False)
        started = True
    yield
    if started:
        ray.shutdown()
