"""Connecting to Ray, and the pool of per-host actors."""

from __future__ import annotations

import os
import socket
import threading
from typing import Any

import ray
from nornir.core.inventory import Host

from nornir_ray.errors import RayClusterUnreachableError
from nornir_ray.wire import detach_host
from nornir_ray.worker import HostActor

# Share of a site resource one actor takes. Workers declare 100 units per site, so one
# worker can hold 10,000 host actors before the resource, not memory, is the limit.
SITE_SHARE = 0.01


def resolve_address(address: str | None) -> str | None:
    """Return `address`, else the `RAY_ADDRESS` environment variable, else None (local Ray)."""
    return address or os.environ.get("RAY_ADDRESS") or None


def check_reachable(address: str, timeout: float = 5.0) -> None:
    """Raise `RayClusterUnreachableError` if nothing accepts TCP connections at `host:port`."""
    host, _, port = address.rpartition(":")
    try:
        with socket.create_connection((host, int(port)), timeout=timeout):
            pass
    except (OSError, ValueError) as error:
        raise RayClusterUnreachableError(address, str(error)) from error


def connect(address: str | None, resources: dict[str, float] | None = None) -> None:
    """Connect this process to Ray once: to the cluster at `address`, or to a new local Ray."""
    if ray.is_initialized():
        return
    if address is not None:
        check_reachable(address)
        ray.init(address=address)
    else:
        ray.init(resources=resources)


def site_available(site: str) -> bool:
    """Return whether a live Ray node declares the resource `site`."""
    return site in ray.cluster_resources()


class ActorPool:
    """One `HostActor` per host name, created on first use and kept until killed."""

    def __init__(self) -> None:
        self._actors: dict[str, Any] = {}
        self._lock = threading.Lock()

    def get(self, host: Host, site: str | None) -> Any:
        """Return the actor of `host`, creating it on a node of `site` if it does not exist."""
        with self._lock:
            actor = self._actors.get(host.name)
            if actor is None:
                resources = {site: SITE_SHARE} if site else {}
                actor = HostActor.options(resources=resources).remote(detach_host(host), site)
                self._actors[host.name] = actor
            return actor

    def cancel(self, host_name: str) -> None:
        """Ask the actor of `host_name` to cancel its current call, if it has one."""
        with self._lock:
            actor = self._actors.get(host_name)
        if actor is not None:
            actor.cancel.remote()

    def forget(self, host_name: str, actor: Any) -> None:
        """Drop `actor` if it is still the actor of `host_name`; the next `get` creates a new one."""
        with self._lock:
            if self._actors.get(host_name) is actor:
                del self._actors[host_name]

    def kill(self, host_name: str, actor: Any = None) -> None:
        """Stop the actor of `host_name` and its connections; the next `get` creates a new one.

        With `actor`, stop it only if it is still the host's actor, so a late kill never
        stops a newer actor of the same host.
        """
        with self._lock:
            current = self._actors.get(host_name)
            if current is None or (actor is not None and current is not actor):
                return
            del self._actors[host_name]
        ray.kill(current, no_restart=True)

    def actor_id(self, host_name: str) -> str | None:
        """Return the Ray actor id of `host_name`, or None if it has no actor."""
        with self._lock:
            actor = self._actors.get(host_name)
        if actor is None:
            return None
        return str(actor._actor_id.hex())
