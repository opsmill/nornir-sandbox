"""How importing nornir_ray configures Ray."""

from __future__ import annotations

import os
import subprocess
import sys


def test_importing_nornir_ray_turns_off_rays_uv_run_integration() -> None:
    # Under `uv run`, Ray copies the project to each worker and runs `uv run` there, which
    # races on creating the copy's virtual environment. Workers must use this environment.
    env = {key: value for key, value in os.environ.items() if key != "RAY_ENABLE_UV_RUN_RUNTIME_ENV"}
    code = "import nornir_ray, ray._private.ray_constants as c; print(c.RAY_ENABLE_UV_RUN_RUNTIME_ENV)"
    output = subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, text=True, check=True)
    assert output.stdout.strip() == "False"
