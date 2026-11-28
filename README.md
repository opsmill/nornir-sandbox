# nornir-sandbox

A containerlab lab with three Arista cEOS devices, and Nornir scripts to back up,
run commands on, troubleshoot and validate the devices. Shared code lives in the
`nornir_sandbox` Python package in `src/`.

```text
            ceos1 (172.24.100.11)
     Et1 /                    \ Et2
  Et1   /                      \   Et2
 ceos2 (172.24.100.12) ---- ceos3 (172.24.100.13)
                      Et2  Et1
```

| Link          | Addresses        |
|---------------|------------------|
| ceos1 – ceos2 | 10.0.12.0/31     |
| ceos1 – ceos3 | 10.0.13.0/31     |
| ceos2 – ceos3 | 10.0.23.0/31     |
| Loopback0     | 10.255.0.1–3/32  |

Login: `admin` / `admin`, SSH on the management address.

## Requirements

- Docker, and containerlab. Your user must be in the `clab_admins` group, or
  set `CONTAINERLAB="sudo containerlab"`.
- The cEOS image `registry.opsmill.io/external/ceos-image:4.29.0.2F`. To use
  another image, change `topology.kinds.ceos.image` in `lab/sandbox.clab.yml`.
- [uv](https://docs.astral.sh/uv/). `uv sync` installs pre-releases of Nornir
  and nornir-scrapli from PyPI (see
  [Pre-releases of Nornir and nornir-scrapli](#pre-releases-of-nornir-and-nornir-scrapli)).
- About 3 GB of free memory for the three devices.

## Start and stop the lab

```bash
uv sync                  # install the package and its dependencies in .venv
uv run invoke up         # deploy, then wait until every device accepts SSH logins
uv run invoke validate   # check links and LLDP neighbors against inventory/hosts.yaml
uv run invoke backup     # save running configurations under backups/
uv run invoke down       # destroy the lab
```

The devices accept logins about 30 seconds after deploy. A globally installed
`inv` (for example `uv tool install invoke`) works too: the tasks run the
scripts and dev tools through `uv run`, in the project environment.

`invoke down` also removes `lab/clab-sandbox/`, so the next `invoke up` boots
the devices from `lab/configs/`. Use `invoke down --keep-files` to keep the
configurations saved on the devices (`write memory`) for the next deploy.

## Invoke tasks

Run `uv run invoke --list` for the full list, and `uv run invoke --help <task>`
for the options of one task.

| Task                         | What it does                                                       |
|------------------------------|--------------------------------------------------------------------|
| `up [--wait 300]`            | Deploy the lab and wait for SSH logins. `--wait 0` skips the wait. |
| `down [--keep-files]`        | Destroy the lab.                                                   |
| `restart`                    | `down` then `up`.                                                  |
| `status`                     | Show the containers, their state and addresses.                    |
| `inventory`                  | Print the Nornir inventory, without connecting to the devices.     |
| `access [--wait N]`          | Check SSH reachability and login on each device.                   |
| `command -c "show ..."`      | Run show commands and print the output. Repeat `-c` for more.      |
| `backup`                     | Save running configurations to `backups/<timestamp>/`.             |
| `validate [--check lldp]`    | Check interfaces and LLDP neighbors against the inventory.         |
| `lint` / `format` / `test`   | ruff and mypy / ruff fixes / unit tests (no lab needed).           |
| `ray-up` / `ray-down`        | Start or stop the Ray cluster of the Ray runner (after `up`).      |
| `ray-status`                 | Show the Ray nodes and their site resources.                       |

`access`, `command`, `backup` and `validate` take `--hosts ceos1,ceos2` to work
on some devices only (comma-separated for invoke, repeated `--hosts` for the
scripts).

## Scripts

The invoke tasks call these scripts. You can also run them directly with
`uv run python scripts/<type>/<script>.py --help`. The scripts are
[cyclopts](https://github.com/BrianPugh/cyclopts) apps: the options come from
the parameters of the `main()` function and their help from its docstring.
`main()` is a coroutine; cyclopts runs it on an asyncio event loop.
Every script that connects to devices takes `--hosts` (repeat it for several
hosts, for example `--hosts ceos1 --hosts ceos2`) and exits with 1 when a task
fails. Most also take `--verbose`, which prints the traceback of failed tasks.
Nornir also writes a log to `nornir.log`.

- `scripts/backup/backup_configs.py`: saves each running configuration to
  `backups/<YYYYmmdd-HHMMSS>/<host>.cfg` and points `backups/latest` at the
  newest run. Compare two runs with `diff -r backups/<older run> backups/latest`.
- `scripts/execute/run_commands.py`: runs show commands and prints the output.
- `scripts/troubleshoot/check_access.py`: checks, for each device, that the SSH
  port answers, that the login works and that EOS runs `show version`, and
  reports which step failed. `--wait N` retries failed devices for N seconds.
- `scripts/troubleshoot/show_inventory.py`: prints the hosts, addresses,
  platform and expected links that the scripts use.
- `scripts/validate/validate_lab.py`: checks that every lab link interface has
  line protocol up and that LLDP neighbors match the links in the inventory.
- `scripts/runner/`: scripts to explore how runs stream progress, cancel and
  time out. See [Streaming, cancellation and deadlines](#streaming-cancellation-and-deadlines).

## Streaming, cancellation and deadlines

The scripts in `scripts/runner/` run against the lab devices with the runner
features of Nornir 3.7.0a1:
progress reported while a task runs, runs started in the background, cancellation,
and deadlines. Each feature has a synchronous script (`*_sync.py`, threaded or
serial runner, `def` tasks) and an asynchronous one (`*_async.py`, asyncio
runner, `async def` tasks), so you can compare them.

| Script                     | Nornir calls                                                             |
|----------------------------|--------------------------------------------------------------------------|
| `stream_progress_sync.py`  | `nr.stream()`; with `--processor`, `nr.run()` and `task_instance_progress` |
| `stream_progress_async.py` | `nr.astream()`; with `--processor`, `await nr.arun()` and `task_instance_progress` |
| `cancel_on_failure_sync.py`  | `nr.submit()`, `handle.results()`, `handle.progress()`, `handle.cancel()` |
| `cancel_on_failure_async.py` | `await nr.asubmit()`, the same handle methods                          |
| `deadlines_sync.py`        | `per_host_timeout` and `timeout` on the runner, or `RunOptions` for one run |
| `deadlines_async.py`       | the same, on the asyncio runner                                          |

Each script defines its task, `collect_facts`, above `main()`, so you can change
the task and run it again. The tasks run three show commands, one after the
other, with a `PartialResult` before and after each. These options change how
the task behaves on each host:

- `--delay N`: each host waits N seconds on the device (`bash sleep`) before
  each command, so that a run lasts long enough to watch or stop.
- `--slow-host ceos3 --slow-delay N`: that host waits N seconds instead.
- `--fail-host ceos2` (stream and cancel scripts): EOS rejects a command on that
  host before its second command, so its task fails while the other hosts are
  still running.

Examples:

```bash
# Progress events as they arrive; leave the loop after 4 events, which cancels the run
uv run python scripts/runner/stream_progress_sync.py --slow-host ceos3 --fail-host ceos2
uv run python scripts/runner/stream_progress_async.py --break-after 4 --delay 2
uv run python scripts/runner/stream_progress_sync.py --runner serial --break-after 5

# Cancel the run when ceos2 fails; print handle.progress() every half second
uv run python scripts/runner/cancel_on_failure_sync.py --fail-host ceos2 --slow-host ceos3 --watch
uv run python scripts/runner/cancel_on_failure_async.py --fail-host ceos1 --workers 1

# ceos3 waits 10 seconds on the device and passes its 4-second deadline
uv run python scripts/runner/deadlines_sync.py --slow-host ceos3
uv run python scripts/runner/deadlines_sync.py --slow-host ceos3 --split-wait 1
uv run python scripts/runner/deadlines_async.py --slow-host ceos3 --deadlines-on run
uv run python scripts/runner/deadlines_sync.py --runner serial --slow-host ceos1 --slow-delay 6 --per-host-timeout 3 --run-timeout 8
```

The runners stop a host at different points:

- The asyncio runner interrupts a cancelled or timed-out host at the call it is
  awaiting, for example in the middle of the device wait.
- The threaded and serial runners stop a host at its next checkpoint: a `yield`
  of the generator task, a call to `task.checkpoint()`, or the start of a
  subtask. A host blocked in a call to the device runs until the call returns.
- When a deadline passes, the threaded runner records the host as timed out at
  once and the run returns on time, but the host's thread keeps running until it
  reaches a checkpoint. `deadlines_sync.py` prints when the task stopped on its
  thread; `--split-wait` adds checkpoints inside the device wait.
- The serial runner checks deadlines only at checkpoints, so a host blocked in a
  call is recorded as timed out when the call returns.
- The serial runner cannot run in the background: `nr.submit()` raises
  `RunnerCapabilityError`. `nr.stream()` works and runs the hosts on the
  caller's thread.

Cancelled and timed-out hosts get one final result each, with
`TaskCancelledError` or `TaskTimeoutError`. Its `started` field is `False` for a
host that never started, and the host is added to `nr.data.failed_hosts`.
Partial results reach the stream, the handle and processors, but never the
`AggregatedResult`.

A host stopped in the middle of a command leaves its scrapli connection in the
middle of that command. In a test, `show hostname` sent right after the asyncio
runner interrupted `bash sleep 5` returned the prompt and the command echo
before its output, which breaks `| json` parsing. The scripts close the
connections when they end, so this does not affect them. Nornir does not close
the connection of an interrupted host. A longer program should close it before
it sends another command to that host: `await close_failed_connections(nr, result)`
on the asyncio runner, or `host.close_connections()` on the threaded and serial
runners.

## Ray runner (prototype)

`src/nornir_ray/` is a prototype of a Nornir runner that runs each host's task on
a [Ray](https://www.ray.io/) actor, to validate the design in
`docs/superpowers/specs/2026-11-25-ray-runner-design.md`. The runner is
registered as `ray`; the six scripts in `scripts/runner/` accept `--runner ray`.

- Each host gets one long-lived Ray actor, which keeps the host's connection open
  between runs. Nornir binds a connection to the mode (sync or asyncio) that opened
  it, so close the connections before you switch a host from sync to async tasks.
- The actor runs on a Ray node that declares the host's site (`data.site` in
  `inventory/hosts.yaml`) as a Ray resource. A host without a site runs on any node.
  A host whose site has no node gets a progress event that names the site, then
  waits until its deadline.
- Cancellation stops a sync task at its next checkpoint and an async task at the
  call it is awaiting, and keeps the actor and its connection. A sync host that
  passes its deadline has its actor stopped, because its call may be blocked; the
  next run creates a new actor and a new connection.
- Without `RAY_ADDRESS`, the runner starts a local Ray on this machine, with no
  sites: every host runs locally.

The lab cluster has one head node and one worker per site, on the lab's management
network:

```bash
uv run invoke up                 # the cluster uses the lab network
uv run invoke ray-up             # head 172.24.100.21, site_a .31, site_b .32; joins this machine
export RAY_ADDRESS=172.24.100.21:6379
uv run python scripts/runner/stream_progress_sync.py --runner ray
uv run invoke ray-down
```

Each final line ends with the node that ran the host, for example
`[node ray-site-a]`.

To learn how the runner places and keeps the actors without the lab, run the demo in
`scripts/ray_demo/` (see its README.md). It starts a Ray cluster with one worker per
site as processes on this machine, and uses simulated devices:

```bash
uv run python scripts/ray_demo/demo_actors.py    # one actor per host, kept between runs
uv run python scripts/ray_demo/demo_pinning.py   # placement by site, and a site worker that joins late
uv run python scripts/ray_demo/demo_rebalance.py # balanced placement as nodes join and leave
```

## Repository layout

```text
inventory/                  Nornir inventory (SimpleInventory)
  hosts.yaml                devices: management address, group, expected links
  groups.yaml               platform and scrapli connection options
  defaults.yaml             username and password
lab/
  sandbox.clab.yml          containerlab topology
  configs/ceosN.cfg         startup configuration of each device
  ray/                      Ray cluster of the Ray runner: image and Compose file
scripts/                    command-line scripts, one directory per type
  backup/ execute/ troubleshoot/ validate/
  runner/                   streaming, cancellation and deadline examples, sync and async
src/nornir_ray/             Ray runner prototype (runner "ray")
src/nornir_sandbox/         shared Python package
  core.py                   init_nornir() and init_nornir_sync(): Nornir object for the lab (runner, time limits), with a host filter
  eos.py                    send(), send_json() and their sync versions, to run commands on EOS
  checks.py                 compares and summarizes device output; no device access
  cli.py                    shared options (--hosts, --verbose, ...), event lines and result summary
tests/                      unit tests; they do not need the lab
tasks.py                    invoke tasks
```

The inventory is plain YAML in `inventory/`. Each host lists its lab links
under `data.links`; the validation script compares them with the interfaces
and LLDP neighbors on the device.

`inventory/hosts.yaml` and `lab/sandbox.clab.yml` describe the same lab. When
you add a device or a link, change both files. `uv run invoke test` fails when
the host names, management addresses or links in the two files do not match.

## Asyncio runner and connections

The scripts use Nornir's `asyncio` runner and the asyncio `scrapli`
connection, except the `*_sync.py` scripts in `scripts/runner/`. Each task is a
coroutine, and all hosts run on one event loop in the script's process, with no
threads. Up to 10 hosts run a task at the same time (`num_workers` of
`init_nornir()`).

- Run tasks with `await nr.arun(task=...)`. `nr.run()` raises
  `AsyncTaskOnSyncRunError` for a coroutine task, and `nr.arun()` raises
  `SyncTaskOnAsyncRunError` for a plain function.
- Inside a task, get the connection with
  `await task.host.aget_connection("scrapli", task.nornir.config)` and await
  its methods, for example `await conn.send_command(...)`. Without a
  `transport` in the inventory, the asyncio connection uses `asyncssh`.
- The tasks from nornir-scrapli are in `nornir_scrapli.async_tasks`, for
  example `send_commands`. The ones in `nornir_scrapli.tasks` are synchronous
  and fail with the asyncio runner.
- Create the Nornir object with `nr = await init_nornir(...)`. It uses
  `AInitNornir`, which loads the inventory in a worker thread, so the event
  loop is not blocked. `init_nornir_sync()` uses `InitNornir`; use it only
  outside the event loop, as the `--hosts` validation in `cli.py` and the
  `*_sync.py` scripts do.
- Run the tasks inside `async with nr:`. On exit, it closes the connections of
  every host, failed hosts included, before the event loop stops.
- Each host may spend 60 seconds on one run (`per_host_timeout` of
  `init_nornir()`). Nornir stops a host that takes longer at the call it is
  awaiting and gives it a failed result with `TaskTimeoutError`; the other
  hosts are not affected. Nornir does not close the connection of that host.
  Before you run another task on the same hosts, call
  `await close_failed_connections(nr, result)`, as
  `scripts/validate/validate_lab.py` does between two checks.
- Code that blocks, such as file writes or `time.sleep()`, stops every host
  while it runs. Use `await asyncio.to_thread(...)` or `await asyncio.sleep()`,
  as in `scripts/backup/backup_configs.py` and `scripts/troubleshoot/check_access.py`.
- `init_nornir_sync(runner="threaded")` or `runner="serial"` returns a Nornir
  object for plain `def` tasks, as in the `*_sync.py` scripts in `scripts/runner/`.
  Open it with `with init_nornir_sync(...) as nr:`, and use `send_sync()` and
  `send_json_sync()` from `nornir_sandbox.eos`. The `scrapli` connection supports both
  modes; Nornir opens it in the mode of the runner.
- `per_host_timeout` and `timeout` of `init_nornir()` and `init_nornir_sync()`
  set the runner's deadlines, in seconds, for each host and for the whole run.
  `None` removes a deadline. `RunOptions` sets them for one run; a value of
  `None` in `RunOptions` keeps the runner's deadline.

### Pre-releases of Nornir and nornir-scrapli

The asyncio runner, `AInitNornir`, and the streaming, cancellation and time
limits of the runners are in Nornir 3.7.0a1.
The asyncio scrapli connection and tasks are in nornir-scrapli 1.6.0a1, which
requires Nornir 3.7.0a1 or later. Both are pre-releases on PyPI, and their
APIs can still change. `pyproject.toml` pins the exact versions, because uv
and pip install a pre-release only when the version is given. To move to a
newer pre-release, change both versions and run `uv lock`. The Nornir guide
[Testing the 3.7 pre-releases](https://github.com/nornir-automation/nornir/blob/v3.7.0a1/docs/upgrading/testing_3_7_prereleases.rst)
lists what changed.

## Add your own code

- A new script: add a file under the matching `scripts/<type>/` directory (or a
  new type directory). Start from `scripts/validate/validate_lab.py`: a
  cyclopts `App`, an `async def main()` function using the `Hosts` and
  `Verbose` option types from `nornir_sandbox.cli`,
  `nr = await init_nornir(hosts=...)`, `async with nr:`, `await nr.arun(...)`, and
  `print_summary(...)` for the output and exit code. For a script that uses the
  threaded or serial runner, start from `scripts/runner/stream_progress_sync.py`:
  a plain `def main()`, `with init_nornir_sync(runner="threaded") as nr:` and
  `def` tasks.
- A new Nornir task: write it in the script that uses it, above `main()`, as
  the existing scripts do. Move it to the `nornir_sandbox` package only when
  several scripts use it. Use `await send()` or `await send_json()` from
  `nornir_sandbox.eos` to run commands on EOS; they raise `EOSCommandError` when
  EOS rejects a command. Put logic that does not need a device in a plain
  function, as in `checks.py`, so it can be unit tested.
- A new Nornir plugin: add it to the package, for example under
  `src/nornir_sandbox/plugins/`, and register it in `pyproject.toml` under
  `[project.entry-points."nornir.plugins.<type>"]`. Connection plugins that
  support asyncio go under `nornir.plugins.capability_connections`. Run
  `uv sync` after changing entry points.
- A shortcut: add an `@task` to `tasks.py`.

## Known behaviors

- The image tagged `4.29.0.2F` reports `EOS-4.28.9M` in `show version`.
- Containerlab uses `lab/configs/ceosN.cfg` only when `lab/clab-sandbox/ceosN/`
  does not exist. After you change a startup configuration, run
  `invoke restart`.
- The devices see each other as LLDP neighbors on `Management0` because they
  share the management network. The LLDP check ignores management interfaces.
