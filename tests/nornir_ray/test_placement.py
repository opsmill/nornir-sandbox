"""Tests of the placement plan: which Ray node runs each host, and which hosts move."""

from __future__ import annotations

import math
from collections import Counter

from nornir_ray.placement import BalancePolicy, Node, Plan, plan


def nodes(*names: str, sites: frozenset[str] = frozenset(), weight: float = 1.0, ready: bool = True) -> list[Node]:
    return [Node(node_id=f"id-{name}", name=name, sites=sites, weight=weight, ready=ready) for name in names]


def site_less(count: int) -> dict[str, str | None]:
    return {f"h{i:03d}": None for i in range(count)}


def loads(result: Plan) -> Counter[str]:
    return Counter(result.assignment.values())


def test_each_host_runs_on_a_node_of_its_site() -> None:
    cluster = nodes("a1", sites=frozenset({"site_a"})) + nodes("b1", "b2", sites=frozenset({"site_b"}))
    hosts = {"ha": "site_a", "hb1": "site_b", "hb2": "site_b", "hb3": "site_b"}

    result = plan(hosts, cluster, {}, BalancePolicy())

    assert result.assignment["ha"] == "id-a1"
    assert {result.assignment[h] for h in ("hb1", "hb2", "hb3")} <= {"id-b1", "id-b2"}
    assert result.unplaced == []


def test_a_node_with_no_weight_gets_no_host() -> None:
    cluster = nodes("head", weight=0.0) + nodes("w1", "w2")

    result = plan(site_less(20), cluster, {}, BalancePolicy())

    assert "id-head" not in result.assignment.values()


def test_new_hosts_spread_within_the_tolerance() -> None:
    cluster = nodes("w1", "w2", "w3", "w4")
    policy = BalancePolicy(tolerance=0.25)

    result = plan(site_less(100), cluster, {}, policy)

    assert max(loads(result).values()) <= math.ceil(25 * 1.25)
    assert all(move.reason == "new" for move in result.moves)


def test_a_node_with_twice_the_weight_gets_about_twice_the_hosts() -> None:
    cluster = nodes("big", weight=2.0) + nodes("small")

    result = plan(site_less(300), cluster, {}, BalancePolicy(tolerance=0.1))

    assert 180 <= loads(result)["id-big"] <= 220


def test_the_plan_does_not_depend_on_the_order_of_the_nodes() -> None:
    cluster = nodes("w1", "w2", "w3")

    first = plan(site_less(50), cluster, {}, BalancePolicy())
    second = plan(site_less(50), list(reversed(cluster)), {}, BalancePolicy())

    assert first.assignment == second.assignment


def test_nothing_moves_when_nothing_changed() -> None:
    cluster = nodes("w1", "w2", "w3")
    first = plan(site_less(60), cluster, {}, BalancePolicy())

    again = plan(site_less(60), cluster, first.assignment, BalancePolicy())

    assert again.moves == []
    assert again.assignment == first.assignment


def test_a_small_imbalance_within_the_tolerance_moves_nothing() -> None:
    cluster = nodes("w1", "w2", "w3", "w4")
    hosts = site_less(40)
    names = sorted(hosts)
    # 12, 10, 9, 9 hosts: within floor(10 * 0.75) = 7 and ceil(10 * 1.25) = 13 per node.
    placed = ["id-w1"] * 12 + ["id-w2"] * 10 + ["id-w3"] * 9 + ["id-w4"] * 9
    current = dict(zip(names, placed, strict=True))

    result = plan(hosts, cluster, current, BalancePolicy(tolerance=0.25))

    assert result.moves == []


def test_hosts_of_a_node_that_left_move_and_no_other_host_moves() -> None:
    cluster = nodes("w1", "w2", "w3", "w4")
    first = plan(site_less(80), cluster, {}, BalancePolicy())

    result = plan(site_less(80), cluster[:3], first.assignment, BalancePolicy(max_moves_ratio=0.0))

    moved = {move.host for move in result.moves}
    assert moved == {host for host, node in first.assignment.items() if node == "id-w4"}
    assert {move.reason for move in result.moves} == {"node unavailable"}
    assert "id-w4" not in result.assignment.values()


def test_a_new_node_receives_at_most_the_move_limit_per_run_then_the_plan_settles() -> None:
    hosts = site_less(90)
    policy = BalancePolicy(tolerance=0.25, max_moves_ratio=0.05)
    current = plan(hosts, nodes("w1", "w2", "w3"), {}, policy).assignment
    cluster = nodes("w1", "w2", "w3", "w4")

    moves_per_run = []
    for _ in range(20):
        result = plan(hosts, cluster, current, policy)
        moves_per_run.append(len(result.moves))
        current = result.assignment

    assert max(moves_per_run) == math.ceil(90 * 0.05)
    assert all(move.target == "id-w4" for move in plan(hosts, cluster, current, policy).moves)
    assert moves_per_run[-1] == 0
    # 90 hosts on 4 nodes: 22.5 on average, so between floor(22.5 * 0.75) = 16 and
    # ceil(22.5 * 1.25) = 29 per node.
    assert max(loads(result).values()) <= 29
    assert loads(result)["id-w4"] >= 16


def test_a_node_in_warm_up_receives_no_host_from_a_rebalance() -> None:
    hosts = site_less(60)
    current = plan(hosts, nodes("w1", "w2"), {}, BalancePolicy()).assignment
    cluster = nodes("w1", "w2") + nodes("w3", ready=False)

    result = plan(hosts, cluster, current, BalancePolicy(max_moves_ratio=1.0))

    assert result.moves == []


def test_a_node_in_warm_up_receives_a_host_that_has_no_other_node() -> None:
    cluster = nodes("a1", sites=frozenset({"site_a"})) + nodes("c1", sites=frozenset({"site_c"}), ready=False)

    result = plan({"hc": "site_c"}, cluster, {}, BalancePolicy())

    assert result.assignment == {"hc": "id-c1"}


def test_a_host_whose_site_has_no_node_is_unplaced() -> None:
    cluster = nodes("a1", sites=frozenset({"site_a"}))

    result = plan({"ha": "site_a", "hd": "site_d"}, cluster, {}, BalancePolicy())

    assert result.unplaced == ["hd"]
    assert "hd" not in result.assignment


def test_hosts_of_a_node_that_left_fill_the_emptiest_nodes_first() -> None:
    cluster = nodes("w1", "w2", "w3", "w4")
    first = plan(site_less(80), cluster, {}, BalancePolicy())

    # A tolerance of 100 % would let one node take all 20 hosts of w4.
    result = plan(site_less(80), cluster[:3], first.assignment, BalancePolicy(tolerance=1.0))

    # 80 hosts on 3 nodes: 26.7 on average.
    assert max(loads(result).values()) <= 27
