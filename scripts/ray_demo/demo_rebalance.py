"""Show the balanced placement: hosts follow the nodes present, without moving more than needed.

The script runs `show_version` on --hosts simulated devices without a site, with
`RayRunner(balance=BalancePolicy(...))`. The runner then chooses the node of each host
before each run (see src/nornir_ray/placement.py). The script goes through three phases:

1. Two worker nodes: the hosts spread over them.
2. A third worker joins. It receives no host during its warm-up (--warm-up seconds).
   After that, each run moves at most --max-moves-ratio of the hosts to it, until no
   node holds more than (1 + --tolerance) times its share.
3. worker-1 leaves. Its hosts move in the next run, all at once: the move limit applies
   only to moves that rebalance, not to hosts that lost their node.

For each run, the script prints the moves by reason, the number of hosts on each node,
and the number of hosts that logged in again. A host that moves gets a new actor, and
so a new login; the other hosts keep their actor and their session.

Example: uv run python scripts/ray_demo/demo_rebalance.py --hosts 30 --max-moves-ratio 0.1
"""

from __future__ import annotations

import math
import tempfile
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

from cyclopts import App
from nornir.core import Nornir
from simlab import add_node, local_cluster, node_of, show_version, wait_until_gone, write_inventory

from nornir_ray import BalancePolicy, RayRunner
from nornir_sandbox import init_nornir_sync

app = App()


@dataclass
class Run:
    label: str
    moves: Counter[str]
    moved: set[str]
    hosts_per_node: Counter[str]
    logins: set[str]
    failed: bool


def run_once(nr: Nornir, runner: RayRunner, label: str) -> Run:
    """Run show_version once, print one line about it, and return what it did."""
    started = time.monotonic()
    result = nr.run(task=show_version)
    elapsed = time.monotonic() - started
    plan = runner.last_plan
    assert plan is not None
    run = Run(
        label=label,
        moves=Counter(move.reason for move in plan.moves),
        moved={move.host for move in plan.moves},
        hosts_per_node=Counter(node_of(multi[0]) or "failed" for multi in result.values()),
        logins={name for name, multi in result.items() if not multi.failed and multi[0].result["new_session"]},
        failed=result.failed,
    )
    moves = ", ".join(f"{count} {reason}" for reason, count in sorted(run.moves.items())) or "none"
    nodes = "  ".join(f"{node} {count:>2}" for node, count in sorted(run.hosts_per_node.items()))
    print(f"{label:<22} {elapsed:5.2f} s  moves: {moves:<24} logins: {len(run.logins):>2}   {nodes}")
    return run


def check(label: str, passed: bool) -> bool:
    print(f"  {'yes' if passed else 'NO ':<4} {label}")
    return passed


@app.default
def main(
    *,
    hosts: int = 30,
    tolerance: float = 0.25,
    max_moves_ratio: float = 0.1,
    warm_up: float = 3.0,
    max_runs: int = 12,
) -> int:
    """Run on 2 nodes, add a third, then remove the first, and show which hosts move.

    Parameters
    ----------
    hosts
        Number of simulated devices.
    tolerance
        A node gives hosts away only when it holds more than (1 + tolerance) times its share.
    max_moves_ratio
        Share of the hosts that one run may move to rebalance.
    warm_up
        Seconds a new node must be present before it receives hosts.
    max_runs
        Most runs to wait for the placement to settle after the third node joins.
    """
    policy = BalancePolicy(tolerance=tolerance, max_moves_ratio=max_moves_ratio, warm_up=warm_up)
    limit = math.ceil(hosts * max_moves_ratio)
    runs: list[Run] = []
    with tempfile.TemporaryDirectory() as tmp, local_cluster(set()) as cluster:
        runner = RayRunner(num_workers=hosts, per_host_timeout=30, balance=policy)
        nr = init_nornir_sync(inventory_dir=write_inventory(Path(tmp), hosts), runner="threaded").with_runner(runner)
        with nr:
            worker_1 = add_node(cluster, "worker-1")
            add_node(cluster, "worker-2")
            print(f"{hosts} hosts; tolerance {tolerance:.0%}; at most {limit} rebalancing moves per run\n")
            print("Phase 1: worker-1 and worker-2")
            runs.append(run_once(nr, runner, "run 1"))

            print(f"\nPhase 2: worker-3 joins (warm-up {warm_up:g} s)")
            add_node(cluster, "worker-3")
            # The warm-up starts when the runner first sees the node: at the start of run 2.
            first_after_join = run_once(nr, runner, "run 2 (in warm-up)")
            runs.append(first_after_join)
            time.sleep(warm_up)
            phase_2 = []
            for number in range(3, 3 + max_runs):
                phase_2.append(run_once(nr, runner, f"run {number}"))
                if not phase_2[-1].moves:
                    break
            runs.extend(phase_2)

            print("\nPhase 3: worker-1 leaves")
            on_worker_1 = {
                name
                for name, node in (runner.last_plan.assignment.items() if runner.last_plan else [])
                if node == worker_1.node_id
            }
            cluster.remove_node(worker_1)
            wait_until_gone("worker-1")
            phase_3 = [run_once(nr, runner, f"run {len(runs) + 1}")]
            phase_3.append(run_once(nr, runner, f"run {len(runs) + 2}"))
            runs.extend(phase_3)

    settled = phase_2[-1]
    print("\nWhat the runs show:")
    passed = [
        check("every run succeeded on every host", not any(run.failed for run in runs)),
        check("worker-3 received no host during its warm-up", not first_after_join.moves),
        check(
            f"no run moved more than {limit} hosts to rebalance",
            all(run.moves["rebalance"] <= limit for run in runs),
        ),
        check(
            f"the placement settled: the last run of phase 2 moved nothing, and no node holds more "
            f"than {math.ceil(hosts / 3 * (1 + tolerance))} hosts",
            not settled.moves and max(settled.hosts_per_node.values()) <= math.ceil(hosts / 3 * (1 + tolerance)),
        ),
        check(
            "when worker-1 left, only its hosts moved, all in the next run",
            phase_3[0].moved == on_worker_1 and phase_3[0].moves["rebalance"] == 0 and not phase_3[1].moves,
        ),
        check("only the hosts that moved logged in again", all(run.logins == run.moved for run in runs)),
    ]
    return 0 if all(passed) else 1


if __name__ == "__main__":
    app()
