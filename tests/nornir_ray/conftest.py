"""Fixtures for the nornir_ray tests: a local Ray with one resource per site."""

from __future__ import annotations

from collections.abc import Iterator

import pytest
import ray

import nornir_ray  # noqa: F401 - configures Ray before its first import

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
