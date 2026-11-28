"""Placement plan: which Ray node runs the actor of each host, and which hosts move.

`plan()` is a pure function of the hosts, the live nodes and the current placement.
It follows four rules, so that the placement follows the number of nodes without moving
hosts more than needed:

1. A host may run only on a node that declares its site (any node for a host without a
   site), with a weight above 0. The weight sets each node's share of the hosts.
2. A host stays on its current node while that node is alive and allowed.
3. A host that must be placed (new host, or its node left) goes to the node with the
   highest rendezvous-hashing score among the nodes below their fair share, else below
   the tolerance limit. The score depends only on the host and the node, so every driver
   computes the same choice.
4. A node that holds more than `(1 + tolerance)` times its share of the hosts gives
   hosts to nodes that have room, and a node that holds less than `(1 - tolerance)`
   times its share takes hosts from nodes above their share: at most `max_moves_ratio`
   of the hosts per run. Within the tolerance, nothing moves. A node in warm-up
   (`ready=False`) receives hosts only when no other node can take them.
"""

from __future__ import annotations

import hashlib
import math
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Literal

MoveReason = Literal["new", "node unavailable", "rebalance"]


@dataclass(frozen=True)
class Node:
    """A live Ray node that can hold host actors."""

    node_id: str
    name: str
    # Names of the node's Ray resources; a host may run here if its site is one of them.
    sites: frozenset[str]
    weight: float = 1.0
    # False while the node is in warm-up: it receives no host from a rebalance.
    ready: bool = True


@dataclass(frozen=True)
class BalancePolicy:
    """How readily the placement moves hosts between nodes.

    Arguments:
        tolerance: Hosts move to rebalance only when a node holds more than
            `1 + tolerance`, or less than `1 - tolerance`, times its share of the hosts
        max_moves_ratio: Share of the hosts that a run may move to rebalance. Hosts whose
            node left always move
        warm_up: Seconds a node must be present before a rebalance gives it hosts

    """

    tolerance: float = 0.25
    max_moves_ratio: float = 0.1
    warm_up: float = 60.0


@dataclass(frozen=True)
class Move:
    host: str
    source: str | None
    target: str
    reason: MoveReason


@dataclass(frozen=True)
class Plan:
    # Node ID of each placed host.
    assignment: dict[str, str]
    moves: list[Move] = field(default_factory=list)
    # Hosts that no live node is allowed to run.
    unplaced: list[str] = field(default_factory=list)


def score(host: str, node: Node) -> float:
    """Return the weighted rendezvous-hashing score of `node` for `host`; higher wins."""
    digest = hashlib.blake2b(f"{host}\0{node.node_id}".encode(), digest_size=8).digest()
    # A uniform number in (0, 1); -weight / ln(u) gives each node a share proportional to its weight.
    uniform = (int.from_bytes(digest, "big") + 1) / (2**64 + 1)
    return -node.weight / math.log(uniform)


def plan(
    hosts: Mapping[str, str | None],
    nodes: Sequence[Node],
    current: Mapping[str, str],
    policy: BalancePolicy,
) -> Plan:
    """Return the placement of `hosts` (host name to site) on `nodes`, from `current`."""
    by_id = {node.node_id: node for node in nodes if node.weight > 0}
    allowed = {name: _allowed(site, by_id.values()) for name, site in sorted(hosts.items())}
    share = _shares(allowed, by_id)
    fair = {node_id: math.ceil(value) for node_id, value in share.items()}
    limit = {node_id: math.ceil(value * (1 + policy.tolerance)) for node_id, value in share.items()}
    floor = {node_id: math.floor(value * (1 - policy.tolerance)) for node_id, value in share.items()}
    assignment: dict[str, str] = {}
    moves: list[Move] = []
    unplaced: list[str] = []

    for name, options in allowed.items():
        if current.get(name) in {node.node_id for node in options}:
            assignment[name] = current[name]
    load = Counter(assignment.values())

    for name, options in allowed.items():
        if name in assignment:
            continue
        if not options:
            unplaced.append(name)
            continue
        ready = [node for node in options if node.ready] or options
        ranked = sorted(ready, key=lambda node: score(name, node), reverse=True)
        target = next(
            (n for n in ranked if load[n.node_id] < fair[n.node_id]),
            next((n for n in ranked if load[n.node_id] < limit[n.node_id]), ranked[0]),
        )
        assignment[name] = target.node_id
        load[target.node_id] += 1
        reason: MoveReason = "node unavailable" if name in current else "new"
        moves.append(Move(name, current.get(name), target.node_id, reason))

    budget = math.ceil(len(hosts) * policy.max_moves_ratio)
    while budget > 0:
        move = _best_rebalance(assignment, allowed, load, fair, floor, limit)
        if move is None:
            break
        assignment[move.host] = move.target
        load[move.source or ""] -= 1
        load[move.target] += 1
        moves.append(move)
        budget -= 1
    return Plan(assignment, moves, unplaced)


def _allowed(site: str | None, nodes: Iterable[Node]) -> list[Node]:
    return [node for node in nodes if site is None or site in node.sites]


def _shares(allowed: Mapping[str, list[Node]], by_id: Mapping[str, Node]) -> dict[str, float]:
    """Return the expected number of hosts on each node.

    A host adds to each ready node it may run on, in proportion to the node's weight
    among those nodes. A node in warm-up has no share yet.
    """
    expected = dict.fromkeys(by_id, 0.0)
    for options in allowed.values():
        ready = [node for node in options if node.ready]
        total = sum(node.weight for node in ready)
        for node in ready:
            expected[node.node_id] += node.weight / total
    return expected


def _best_rebalance(
    assignment: Mapping[str, str],
    allowed: Mapping[str, list[Node]],
    load: Counter[str],
    fair: Mapping[str, int],
    floor: Mapping[str, int],
    limit: Mapping[str, int],
) -> Move | None:
    """Return the next host to move to rebalance, or None if every node is within the tolerance.

    First, a host leaves the most overloaded node for a node below its fair share, else
    for a node below its limit. Then, the most underloaded node takes a host from a node
    above its fair share. Among the candidates, the host that prefers its new node the
    most moves, so that the placement converges towards the rendezvous-hashing choice.
    """
    for source in sorted((n for n in limit if load[n] > limit[n]), key=lambda n: (limit[n] - load[n], n)):
        for room in (fair, limit):
            move = _preferred([source], [n for n in limit if n != source and load[n] < room[n]], assignment, allowed)
            if move is not None:
                return move
    for target in sorted((n for n in floor if load[n] < floor[n]), key=lambda n: (load[n] - floor[n], n)):
        donors = [n for n in fair if n != target and load[n] > fair[n]]
        move = _preferred(donors, [target], assignment, allowed)
        if move is not None:
            return move
    return None


def _preferred(
    sources: Sequence[str], targets: Sequence[str], assignment: Mapping[str, str], allowed: Mapping[str, list[Node]]
) -> Move | None:
    """Return the move of a host from `sources` to a ready node of `targets` with the best score gain."""
    best: tuple[float, str, str, str] | None = None
    for host in sorted(h for h, n in assignment.items() if n in sources):
        options = {node.node_id: node for node in allowed[host]}
        current = options[assignment[host]]
        for target in sorted(targets):
            node = options.get(target)
            if node is None or not node.ready:
                continue
            gain = score(host, node) / score(host, current)
            if best is None or gain > best[0]:
                best = (gain, host, current.node_id, target)
    return Move(best[1], best[2], best[3], "rebalance") if best else None
