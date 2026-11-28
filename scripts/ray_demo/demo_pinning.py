"""Show how the Ray runner places each host on the worker of its site, and keeps it there.

The script starts a Ray cluster on this machine with a worker node for each site of the
inventory except --late-site (site_c), and prints the nodes and the site each one
declares. It then streams a run of `show_version`:

- The actor of each host whose `data.site` names a running worker starts on that worker.
- rtr-x1 has no site, so its actor can start on any node.
- rtr-c1 is in site_c, which has no worker yet: the run reports that the host waits,
  and the host runs as soon as the site_c worker joins, --late-after seconds later.

A second run then shows that each host runs on the same node, in the same actor, with
the same session, because its actor stays where it was created.

Example: uv run python scripts/ray_demo/demo_pinning.py --late-after 5
"""

from __future__ import annotations

import threading
import time
from collections import defaultdict
from typing import Any

from cyclopts import App
from nornir.core import Nornir
from nornir.core.task import AggregatedResult, PartialResult, Result
from simlab import (
    INVENTORY_DIR,
    add_worker,
    host_name,
    local_cluster,
    node_of,
    print_nodes,
    show_version,
    site_of,
    sites_of,
    worker_name,
)

from nornir_sandbox import init_nornir_sync

app = App()

# Seconds a host may wait for its worker and run, before the run fails it.
PER_HOST_TIMEOUT = 30.0


def describe(result: Result) -> str:
    facts: dict[str, Any] = result.result
    return f"on {node_of(result) or '-':<14} actor {facts['actor']}  pid {facts['pid']}  session {facts['session']}"


def stream(nr: Nornir, started: float) -> AggregatedResult:
    """Run show_version with nr.stream(), print each event as it arrives, and return the results."""
    events = nr.stream(task=show_version)
    for event in events:
        elapsed = time.monotonic() - started
        if isinstance(event, PartialResult):
            print(f"{elapsed:6.2f}s  {host_name(event):<7} {event.result}")
        elif event.failed:
            print(f"{elapsed:6.2f}s  {host_name(event):<7} failed: {event.exception!r}")
        else:
            print(f"{elapsed:6.2f}s  {host_name(event):<7} ran {describe(event)}")
    return events.result()


def print_placement(result: AggregatedResult) -> None:
    by_node: dict[str, list[str]] = defaultdict(list)
    for name, multi in sorted(result.items()):
        site = site_of(multi[0]) or "no site"
        by_node[node_of(multi[0]) or "(failed)"].append(f"{name} ({site})")
    print("\nHosts on each node:")
    for node, hosts in sorted(by_node.items()):
        print(f"  {node:<15} {', '.join(hosts)}")


@app.default
def main(*, late_site: str = "site_c", late_after: float = 4.0) -> int:
    """Stream a run on simulated devices while the worker of one site joins late.

    Parameters
    ----------
    late_site
        Site whose worker joins the cluster after the run starts.
    late_after
        Seconds after the start of the run when the worker of --late-site joins.
    """
    nr = init_nornir_sync(inventory_dir=INVENTORY_DIR, runner="ray", per_host_timeout=PER_HOST_TIMEOUT)
    sites = sites_of(nr.inventory.hosts.values())
    with local_cluster(sites - {late_site}) as cluster, nr:
        print_nodes()

        print(f"\nRun 1: the worker of {late_site} ({worker_name(late_site)}) joins after {late_after:g} s\n")
        # Ray adds a node only from the main thread, so the run streams in another thread.
        results: list[AggregatedResult] = []
        started = time.monotonic()
        run = threading.Thread(target=lambda: results.append(stream(nr, started)))
        run.start()
        time.sleep(late_after)
        print(f"{time.monotonic() - started:6.2f}s  {worker_name(late_site)} is starting")
        add_worker(cluster, late_site)
        print(f"{time.monotonic() - started:6.2f}s  {worker_name(late_site)} joined the cluster")
        run.join()
        first = results[0]
        print_nodes()
        print_placement(first)

        print("\nRun 2: the same hosts again\n")
        second = stream(nr, time.monotonic())

    print("\nWhat the runs show:")
    passed = [
        check("every host succeeded on both runs", not first.failed and not second.failed),
        check(
            "each host with a site ran on the worker of its site",
            all(
                node_of(multi[0]) == worker_name(site_of(multi[0]) or "")
                for multi in first.values()
                if site_of(multi[0])
            ),
        ),
        check(
            "run 2 used the same node, actor and session as run 1 for every host",
            all(placement(first[name][0]) == placement(second[name][0]) for name in first),
        ),
    ]
    return 0 if all(passed) else 1


def placement(result: Result) -> tuple[Any, ...]:
    if result.failed:
        return (None,)
    return (node_of(result), result.result["actor"], result.result["session"])


def check(label: str, passed: bool) -> bool:
    print(f"  {'yes' if passed else 'NO ':<4} {label}")
    return passed


if __name__ == "__main__":
    app()
