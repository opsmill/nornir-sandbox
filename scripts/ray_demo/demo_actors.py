"""Run the same task several times and show what the actor of each host keeps between runs.

The script starts a Ray cluster on this machine (a head node and one worker node per
site of the inventory), then runs `show_version` --runs times on the same Nornir object
with the Ray runner. For each run and each host, it prints:

- node: the Ray node that ran the task. It is the worker of the host's site.
- actor and pid: the Ray actor of the host and its process. Each host has its own actor,
  created on the first run and reused by the later runs.
- session and login: the session on the simulated device. The first run logs in
  (LOGIN_SECONDS); the later runs reuse the session that the actor keeps open.
- commands: the number of commands sent on that session so far.

With --runner threaded, the same task runs in threads of this process, for comparison:
every host shows this script's process ID, and there is no Ray node or actor.

Example: uv run python scripts/ray_demo/demo_actors.py --runs 3
"""

from __future__ import annotations

import os
import time
from contextlib import nullcontext
from typing import Any, Literal

from cyclopts import App
from nornir.core.task import AggregatedResult
from simlab import INVENTORY_DIR, local_cluster, node_of, show_version, site_of, sites_of, worker_name

from nornir_sandbox import init_nornir_sync

app = App()

COLUMNS = f"{'host':<7} {'site':<7} {'node':<14} {'actor':<9} {'pid':>7}  {'session':<8} {'login':<7} commands"


def print_run(result: AggregatedResult) -> None:
    print(COLUMNS)
    for name, multi in sorted(result.items()):
        if multi.failed:
            print(f"{name:<7} failed: {multi[0].exception!r}")
            continue
        facts: dict[str, Any] = multi[0].result
        site = site_of(multi[0]) or "-"
        node = node_of(multi[0]) or "this process"
        login = "new" if facts["new_session"] else "reused"
        print(
            f"{name:<7} {site:<7} {node:<14} {facts['actor'] or '-':<9} {facts['pid']:>7}  "
            f"{facts['session']:<8} {login:<7} {facts['commands']}"
        )


def check(label: str, passed: bool) -> bool:
    print(f"  {'yes' if passed else 'NO ':<4} {label}")
    return passed


def print_checks(runs: list[AggregatedResult], ray_runner: bool) -> bool:
    """Print what the runs show, and return False if a run does not show it."""
    first, *later = runs
    hosts = sorted(first)
    facts = {name: [run[name][0].result for run in runs] for name in hosts}
    results = [check("every host succeeded on every run", not any(run.failed for run in runs))]
    results.append(
        check(
            "each host kept the same process and session on every run",
            all(len({(f["pid"], f["session"]) for f in facts[name]}) == 1 for name in hosts),
        )
    )
    results.append(
        check("only the first run logged in to the devices", not any(f["new_session"] for f in facts_of(later)))
    )
    if ray_runner:
        results.append(
            check(
                "each host has its own actor and process",
                len({facts[name][0]["pid"] for name in hosts}) == len(hosts),
            )
        )
        results.append(
            check(
                "each host with a site ran on its site's worker on every run",
                all(
                    node_of(run[name][0]) == worker_name(site_of(run[name][0]) or "")
                    for run in runs
                    for name in hosts
                    if site_of(run[name][0])
                ),
            )
        )
    else:
        results.append(
            check("every host ran in this script's process", all(f["pid"] == os.getpid() for f in facts_of(runs)))
        )
    return all(results)


def facts_of(runs: list[AggregatedResult]) -> list[dict[str, Any]]:
    return [multi[0].result for run in runs for multi in run.values()]


@app.default
def main(*, runs: int = 3, runner: Literal["ray", "threaded"] = "ray") -> int:
    """Run show_version several times on simulated devices and show where each host ran.

    Parameters
    ----------
    runs
        Number of runs on the same Nornir object.
    runner
        "ray" runs each host on its actor; "threaded" runs the hosts in threads of this
        process, for comparison.
    """
    nr = init_nornir_sync(inventory_dir=INVENTORY_DIR, runner=runner)
    sites = sites_of(nr.inventory.hosts.values())
    print(f"This script's process ID: {os.getpid()}")
    if runner == "ray":
        print(f"Starting a Ray cluster on this machine: head, {', '.join(worker_name(s) for s in sorted(sites))}")
    results = []
    with local_cluster(sites) if runner == "ray" else nullcontext(), nr:
        for number in range(1, runs + 1):
            started = time.monotonic()
            result = nr.run(task=show_version)
            print(f"\nRun {number} took {time.monotonic() - started:.2f} s")
            print_run(result)
            results.append(result)
    print("\nWhat the runs show:")
    return 0 if print_checks(results, runner == "ray") else 1


if __name__ == "__main__":
    app()
