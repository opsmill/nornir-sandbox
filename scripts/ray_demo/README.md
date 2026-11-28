# Ray runner demo on simulated devices

Three scripts that run the Ray runner (`nornir_ray`) on this machine, without the lab and
without Docker. They show what the runner does with each host:

- `demo_actors.py` runs the same task several times and shows that each host has its own
  Ray actor, a process that keeps the host's connection open between runs.
- `demo_pinning.py` shows that the actor of each host starts on the Ray worker of the
  host's site and stays there, and that a host whose site has no worker waits for one.
- `demo_rebalance.py` shows the balanced placement (`RayRunner(balance=...)`): the
  runner chooses the node of each host, so the hosts follow the nodes that join and
  leave, without moving more hosts than needed.

```bash
uv run python scripts/ray_demo/demo_actors.py
uv run python scripts/ray_demo/demo_actors.py --runner threaded   # the same task without Ray
uv run python scripts/ray_demo/demo_pinning.py
uv run python scripts/ray_demo/demo_rebalance.py
```

Each script ends with a list of checks ("What the runs show"), and returns 1 when one of
them fails.

## What runs where

`simlab.local_cluster()` starts a Ray cluster as processes on this machine:

- a head node named `head`, with no CPUs;
- one worker node per site of the inventory, named `worker-site-a`, `worker-site-b` and
  so on. Each declares its site as a Ray custom resource (`{"site_a": 100}`), as the
  containers of `lab/ray/compose.yml` do.

The script itself is the Ray driver. For each host, the Ray runner creates one actor that
requires a small share of the resource named in the host's `data.site`, so Ray can start
it only on the worker of that site. A host without a site can start on any node.

The devices are simulated: `inventory/hosts.yaml` lists seven hosts on documentation
addresses, and the `simulated` connection plugin in `simlab.py` stands in for a device.
Opening it waits one second, as a login would, and gives the session a random ID. The task
`show_version` returns the process ID, the actor ID and the session ID, so the scripts
can show where each host ran.

## demo_actors.py: one actor per host, kept between runs

Example output of the first two runs:

```text
Run 1 took 1.57 s
host    site    node           actor         pid  session  login   commands
rtr-a1  site_a  worker-site-a  032ea50b  2914761  4c0dd0   new     1
rtr-a2  site_a  worker-site-a  b99be65d  2914864  fdca66   new     1
rtr-a3  site_a  worker-site-a  5bded630  2914983  503448   new     1
rtr-b1  site_b  worker-site-b  6f25023d  2914762  ce78e3   new     1
rtr-b2  site_b  worker-site-b  f4f6d4d3  2914853  09eae1   new     1
rtr-c1  site_c  worker-site-c  d6a77700  2914852  0eae15   new     1
rtr-x1  -       worker-site-c  afa97ea9  2914763  f30478   new     1

Run 2 took 0.01 s
host    site    node           actor         pid  session  login   commands
rtr-a1  site_a  worker-site-a  032ea50b  2914761  4c0dd0   reused  2
rtr-a2  site_a  worker-site-a  b99be65d  2914864  fdca66   reused  2
...
```

What to look at:

- Each host has its own actor ID and process ID, and they do not change between runs.
- The first run takes about one second, because each actor logs in to its device. The
  later runs reuse the session that the actor keeps open, so `commands` grows on the same
  session ID.
- With `--runner threaded`, every host shows this script's process ID: Nornir keeps the
  connections in this process instead of in one actor per host.

## demo_pinning.py: placement by site

The script starts the workers of site_a and site_b only, then streams a run. rtr-c1 is in
site_c, which has no worker, so the run reports that it waits. Four seconds later
(`--late-after`) the script adds `worker-site-c`, and rtr-c1 runs on it:

```text
  0.01s  rtr-c1  no Ray worker for site site_c; waiting
  1.24s  rtr-x1  ran on head           actor 76280adb  pid 2913383  session 48c28b
  1.24s  rtr-a2  ran on worker-site-a  actor 6f35d9dd  pid 2913381  session 510f6c
  1.24s  rtr-b1  ran on worker-site-b  actor 78504b35  pid 2913382  session 4eeac4
  ...
  4.00s  worker-site-c is starting
  5.11s  worker-site-c joined the cluster
  5.47s  rtr-c1  ran on worker-site-c  actor a39947ca  pid 2913926  session d84f85
```

rtr-x1 has no site: Ray starts its actor on any node, here the head node. A second run
then shows each host on the same node, actor and session as in the first run, because
an actor stays on the node where Ray started it. A host waits for its site's worker for at
most 30 seconds (the per-host time limit), then fails with `TaskTimeoutError`.

Things to try:

- Change the `site` of a host in `inventory/hosts.yaml`, or add a host, and run the
  scripts again.
- Run `demo_pinning.py --late-site site_a` to make all the site_a hosts wait.

## demo_rebalance.py: hosts follow the nodes present

Without `balance`, Ray places the actor of a host on a node of its site once, and the
actor stays there: a node that joins later receives no host. With
`RayRunner(balance=BalancePolicy(...))`, the runner chooses the node of each host before
each run (`src/nornir_ray/placement.py`):

- A host may run only on a node of its site (any node for a host without a site) that
  has CPUs. A node's share of the hosts is proportional to its number of CPUs.
- A host stays on its node while the node is alive. A host that must be placed goes to
  the node that rendezvous hashing prefers for it, among the nodes below their share.
  The hashing depends only on the host and the node, so the choice does not depend on
  the order of the nodes or on which script computes it.
- Hosts move to rebalance only when a node holds more than `1 + tolerance`, or less than
  `1 - tolerance`, times its share; at most `max_moves_ratio` of the hosts per run.
- A node that joins receives hosts only after `warm_up` seconds, counted from the first
  run that sees it.
- The hosts of a node that left all move in the next run, whatever the move limit.
- A host moves between runs, never during one: the runner stops its actor, and the run
  creates a new actor on the new node, which logs in again.

The script generates 30 hosts without a site, and goes through three phases:

```text
30 hosts; tolerance 25%; at most 3 rebalancing moves per run

Phase 1: worker-1 and worker-2
run 1                   2.06 s  moves: 30 new                   logins: 30   worker-1 15  worker-2 15

Phase 2: worker-3 joins (warm-up 3 s)
run 2 (in warm-up)      0.02 s  moves: none                     logins:  0   worker-1 15  worker-2 15
run 3                   1.24 s  moves: 3 rebalance              logins:  3   worker-1 13  worker-2 14  worker-3  3
run 4                   1.24 s  moves: 3 rebalance              logins:  3   worker-1 13  worker-2 11  worker-3  6
run 5                   1.23 s  moves: 1 rebalance              logins:  1   worker-1 12  worker-2 11  worker-3  7
run 6                   0.02 s  moves: none                     logins:  0   worker-1 12  worker-2 11  worker-3  7

Phase 3: worker-1 leaves
run 7                   1.49 s  moves: 12 node unavailable      logins: 12   worker-2 15  worker-3 15
run 8                   0.02 s  moves: none                     logins:  0   worker-2 15  worker-3 15
```

worker-3 stops at 7 hosts, not 10: with a tolerance of 25 %, 7 is the fewest hosts a
node may hold before hosts move to it (`floor(10 * 0.75)`). Try `--tolerance 0` for an
even spread, at the cost of more moves, or `--max-moves-ratio 1` to rebalance in one run.

## Limits of the demo

- Each worker node declares 4 CPUs (`WORKER_CPUS`). The actors use no CPU, but Ray
  starts at most as many worker processes at a time on a node as the node has CPUs, so
  with 1 CPU the actors of the first run start one after the other.
- All the Ray nodes run on this machine. In the lab, each worker is a container on the
  management network (`uv run invoke ray-up`), and the scripts in `scripts/runner/` run on
  the cEOS devices with `--runner ray`.
- The actors run the code of `simlab.py` by value (`cloudpickle.register_pickle_by_value`),
  because they cannot import a module from this directory.
- The scripts import Ray only through `simlab.py`, which imports `nornir_ray` first. If a
  script imports `ray` before `nornir_ray`, Ray's `uv run` integration stays on, Ray prints
  warnings about packaging the working directory, and each run adds about 350 MB under
  `/tmp/ray`.
