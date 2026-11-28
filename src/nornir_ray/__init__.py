"""Ray runner for Nornir: runs each host's task on a Ray actor placed by site."""

import os

# Ray reads this when it is first imported. Under `uv run`, Ray would otherwise copy the
# project to each worker and start it with `uv run`, and concurrent workers race on
# creating that copy's virtual environment. Workers use the driver's environment (local
# Ray) or the image's (lab cluster) instead.
os.environ.setdefault("RAY_ENABLE_UV_RUN_RUNTIME_ENV", "0")

from nornir_ray.errors import (
    HostActorDiedError,
    RayClusterUnreachableError,
    RemoteTaskError,
    TaskNotSerializableError,
)
from nornir_ray.placement import BalancePolicy
from nornir_ray.runner import RayRunner

__all__ = [
    "BalancePolicy",
    "HostActorDiedError",
    "RayClusterUnreachableError",
    "RayRunner",
    "RemoteTaskError",
    "TaskNotSerializableError",
]
