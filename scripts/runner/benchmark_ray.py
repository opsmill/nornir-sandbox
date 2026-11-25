"""Measure the time the Ray runner adds to a run, against the threaded runner.

On the lab (default), each host runs `show version | json`. With --simulated, the
script writes an inventory of --hosts-count hosts split between site_a and site_b
and runs a task that sleeps 0.05 s and reports two partial results, without devices.

For each runner, the script prints the cold run (the first run: for Ray, it creates
one actor per host and opens the connections) and the median of --repeat warm runs.
With RAY_ADDRESS set, it also prints the memory used by the Ray containers after the
runs, read with `docker stats`.
"""

from __future__ import annotations

import statistics
import subprocess
import tempfile
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

from cyclopts import App
from nornir.core import Nornir
from nornir.core.task import PartialResult, Result, Task
from nornir.plugins.runners import ThreadedRunner

from nornir_ray import RayRunner
from nornir_sandbox import init_nornir_sync
from nornir_sandbox.eos import send_json_sync
from nornir_sandbox.paths import INVENTORY_DIR

app = App()


def show_version(task: Task) -> Result:
    """Run `show version | json` on the device."""
    return Result(host=task.host, result=send_json_sync(task, "show version")["version"])


def simulated(task: Task) -> Iterator[PartialResult]:
    """Sleep 0.05 s in two steps, with a partial result after each, without a device."""
    for step in (1, 2):
        time.sleep(0.025)
        yield PartialResult(task.host, f"step {step}", progress=step / 2)
    return Result(host=task.host, result="done")  # type: ignore[return-value]


def write_inventory(directory: Path, count: int) -> Path:
    """Write `count` hosts split between site_a and site_b; addresses are never used."""
    hosts = "".join(
        f"sim{i:04d}:\n  hostname: 192.0.2.{i % 250 + 1}\n  data: {{site: site_{'a' if i % 2 else 'b'}}}\n"
        for i in range(count)
    )
    (directory / "hosts.yaml").write_text("---\n" + hosts)
    (directory / "groups.yaml").write_text("---\n{}\n")
    (directory / "defaults.yaml").write_text("---\n{}\n")
    return directory


def timed_runs(nr: Nornir, task: Callable[..., Any], repeat: int) -> tuple[float, list[float]]:
    """Return the duration of the first run and of `repeat` more runs. Raise if a host fails."""
    durations = []
    for _ in range(repeat + 1):
        started = time.monotonic()
        result = nr.run(task=task, on_failed=True)
        durations.append(time.monotonic() - started)
        if result.failed:
            failed = {name: str(multi[0].exception) for name, multi in result.items() if multi.failed}
            raise RuntimeError(f"hosts failed: {failed}")
    return durations[0], durations[1:]


def ray_container_memory() -> str:
    """Return the memory of each Ray container, as `docker stats` reports it."""
    output = subprocess.run(
        ["docker", "stats", "--no-stream", "--format", "{{.Name}} {{.MemUsage}}"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    return "\n".join(f"  {line}" for line in output.splitlines() if "nornir-sandbox-ray" in line)


@app.default
def main(*, simulated_hosts: bool = False, hosts_count: int = 3, repeat: int = 10) -> int:
    """Time cold and warm runs on the Ray runner and on the threaded runner.

    Args:
        simulated_hosts: run a task without devices on a generated inventory.
        hosts_count: number of generated hosts with --simulated-hosts.
        repeat: number of warm runs per runner.
    """
    with tempfile.TemporaryDirectory() as tmp:
        inventory = write_inventory(Path(tmp), hosts_count) if simulated_hosts else INVENTORY_DIR
        task = simulated if simulated_hosts else show_version
        nr = init_nornir_sync(inventory_dir=inventory, runner="threaded")
        count = len(nr.inventory.hosts)
        print(f"{count} hosts, task {task.__name__}, {repeat} warm runs per runner\n")
        print(f"{'runner':<9} {'cold s':>8} {'warm s':>8} {'warm min':>9} {'warm max':>9}")
        timings = {}
        for name, runner in (("threaded", ThreadedRunner(num_workers=count)), ("ray", RayRunner(num_workers=count))):
            with nr.with_runner(runner) as runner_nr:
                cold, warm = timed_runs(runner_nr, task, repeat)
            timings[name] = (cold, statistics.median(warm))
            print(f"{name:<9} {cold:8.3f} {statistics.median(warm):8.3f} {min(warm):9.3f} {max(warm):9.3f}")
        extra_cold = timings["ray"][0] - timings["threaded"][0]
        extra_warm = timings["ray"][1] - timings["threaded"][1]
        print(f"\nRay adds {extra_cold:.3f} s to a cold run and {extra_warm * 1000:.0f} ms to a warm run")
        print(f"({extra_warm / count * 1000:.2f} ms per host on a warm run)")
    if subprocess.run(["docker", "info"], capture_output=True).returncode == 0:
        memory = ray_container_memory()
        if memory:
            print(f"\nRay containers after the runs:\n{memory}")
    return 0


if __name__ == "__main__":
    app()
