"""Shared code of the Ray runner demo: a Ray cluster on this machine, and simulated devices.

`local_cluster()` starts a Ray head node and one Ray worker node per site, all as
processes on this machine, and connects the script to them. Each worker node declares
its site as a Ray custom resource, as the containers of `lab/ray/compose.yml` do, so the
Ray runner places the actor of a host whose `data.site` is site_a on worker-site-a.

The "simulated" connection plugin stands in for a device: opening it waits
LOGIN_SECONDS, as a login would, and gives the session a random ID. The `show_version`
task reports where it ran (Ray node, actor, process ID) and which session it used, so
the scripts can show what the actor of each host keeps between runs.
"""

from __future__ import annotations

import os
import sys
import time
import uuid
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# Imported before Ray: it turns off Ray's `uv run` integration, with which every Ray
# worker would build its own copy of the project's environment. For the same reason, the
# demo scripts import Ray only through this module.
import nornir_ray  # noqa: F401  # isort: skip
import ray
from nornir.core.task import PartialResult, Result, Task
from ray import cloudpickle
from ray.cluster_utils import Cluster

# The actors run the tasks and the plugin of this module, but cannot import it: send its
# code with each call instead.
cloudpickle.register_pickle_by_value(sys.modules[__name__])

INVENTORY_DIR = Path(__file__).parent / "inventory"
LOGIN_SECONDS = 1.0
WORKER_CPUS = 4
CONNECTION = "simulated"


def worker_name(site: str) -> str:
    """Return the Ray node name of the worker of `site`, for example worker-site-a."""
    return "worker-" + site.replace("_", "-")


def add_worker(cluster: Cluster, site: str) -> None:
    """Start a Ray worker node for `site` on this machine and wait until it joins."""
    add_node(cluster, worker_name(site), {site: 100})


def add_node(cluster: Cluster, name: str, resources: dict[str, float] | None = None) -> Any:
    """Start a Ray worker node named `name`, wait until it joins, and return it.

    The actors need no CPU, but a node starts at most as many worker processes at once as
    it has CPUs, so the node declares WORKER_CPUS. Call it from the main thread: Ray
    installs a signal handler when it starts a node.
    """
    return cluster.add_node(num_cpus=WORKER_CPUS, resources=resources or {}, node_name=name)


def wait_until_gone(name: str, timeout: float = 60.0) -> None:
    """Wait until Ray reports the node named `name` as dead."""
    deadline = time.monotonic() + timeout
    while any(node["Alive"] and node["NodeName"] == name for node in ray.nodes()):
        if time.monotonic() > deadline:
            raise TimeoutError(f"Ray still reports {name} as alive after {timeout:g} s")
        time.sleep(0.2)


def write_inventory(directory: Path, count: int) -> Path:
    """Write an inventory of `count` simulated devices without a site, and return its directory."""
    hosts = "".join(f"dev{i:02d}:\n  hostname: 192.0.2.{i}\n" for i in range(1, count + 1))
    (directory / "hosts.yaml").write_text("---\n" + hosts)
    (directory / "groups.yaml").write_text("---\n{}\n")
    (directory / "defaults.yaml").write_text("---\nusername: demo\npassword: demo\n")
    return directory


@contextmanager
def local_cluster(sites: Iterable[str]) -> Iterator[Cluster]:
    """Start a head node and one worker node per site, connect to them, and stop them on exit.

    The head node has no CPUs, as in the lab. The actors need no CPU, so the actor of a
    host without a site can still run on the head node.
    """
    cluster = Cluster(
        initialize_head=True,
        head_node_args={"num_cpus": 0, "node_name": "head", "include_dashboard": False},
    )
    try:
        for site in sorted(sites):
            add_worker(cluster, site)
        # The Ray runner reuses this connection instead of starting its own local Ray.
        ray.init(address=cluster.address, log_to_driver=False, logging_level="WARNING")
        yield cluster
    finally:
        ray.shutdown()
        cluster.shutdown()


def print_nodes() -> None:
    """Print each live Ray node and the sites it declares."""
    print(f"\n{'Ray node':<15} sites")
    for node in sorted(ray.nodes(), key=lambda n: n["NodeName"]):
        if node["Alive"]:
            sites = sorted(name for name in node["Resources"] if name.startswith("site_"))
            print(f"{node['NodeName']:<15} {', '.join(sites) or '(none: any host without a site)'}")


def host_name(result: Result | PartialResult) -> str:
    return result.host.name if result.host else "-"


def site_of(result: Result) -> str | None:
    """Return the site in the `data.site` of the host of `result`, or None."""
    return result.host.data.get("site") if result.host else None


def node_of(result: Result) -> str | None:
    """Return the Ray node that ran `result`, which the Ray runner adds as `ray_node`, or None."""
    return getattr(result, "ray_node", None)


def sites_of(hosts: Iterable[Any]) -> set[str]:
    """Return the sites named in the `data.site` of `hosts`."""
    return {host.data["site"] for host in hosts if host.data.get("site")}


@dataclass
class SimulatedSession:
    """The state of a login to a simulated device."""

    hostname: str
    session_id: str = field(default_factory=lambda: uuid.uuid4().hex[:6])
    commands: int = 0

    def send(self, command: str) -> str:
        self.commands += 1
        return f"{command}: simulated output of {self.hostname}"


class SimulatedDevice:
    """Connection plugin "simulated": opening it waits LOGIN_SECONDS and starts a session."""

    def __init__(self) -> None:
        self._connection: SimulatedSession | None = None

    def get_capabilities(self) -> frozenset[str]:
        return frozenset({"sync"})

    @property
    def connection(self) -> SimulatedSession | None:
        return self._connection

    def open(self, hostname: str | None, *args: Any, **kwargs: Any) -> None:
        time.sleep(LOGIN_SECONDS)
        self._connection = SimulatedSession(hostname or "unknown")

    def close(self) -> None:
        self._connection = None


def register_plugin() -> None:
    """Register the "simulated" connection plugin in this process.

    The registry is per process, so each actor registers the plugin the first time it runs
    the task. It is imported here so that the task does not carry a copy of the registry.
    """
    from nornir.core.plugins.connections import CapabilityConnectionPluginRegister

    if CONNECTION not in CapabilityConnectionPluginRegister.available:
        CapabilityConnectionPluginRegister.register(CONNECTION, SimulatedDevice)  # type: ignore[arg-type]


def show_version(task: Task) -> Result:
    """Send `show version` to the simulated device and report where the task ran.

    The result is a dict: the process ID, the Ray actor ID (None outside Ray), the session
    ID, whether this run opened the session, and the number of commands the session has sent.
    """
    register_plugin()
    new_session = CONNECTION not in task.host.connections
    session: SimulatedSession = task.host.get_connection(CONNECTION, task.nornir.config)
    session.send("show version")
    actor_id = ray.get_runtime_context().get_actor_id() if ray.is_initialized() else None
    return Result(
        host=task.host,
        result={
            "pid": os.getpid(),
            # The first characters of an actor ID are unique; the last ones are the job ID.
            "actor": actor_id[:8] if actor_id else None,
            "session": session.session_id,
            "new_session": new_session,
            "commands": session.commands,
        },
    )
