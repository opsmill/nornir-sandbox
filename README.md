# nornir-sandbox

A containerlab lab with three Arista cEOS devices and one Nokia SR Linux device,
and Nornir scripts to back up, run commands on, troubleshoot and validate the
devices. Shared code lives in the `nornir_sandbox` Python package in `src/`.

```text
                      srl1 (172.24.100.21)
              e1-1 /                    \ e1-2
          Et3     /                      \     Et3
 ceos1 (172.24.100.11) ------------ ceos2 (172.24.100.12)
          Et2     \  Et1        Et1  /     Et2
              Et2  \                /  Et1
                      ceos3 (172.24.100.13)
```

| Link          | Addresses        |
|---------------|------------------|
| ceos1 – ceos2 | 10.0.12.0/31     |
| ceos1 – ceos3 | 10.0.13.0/31     |
| ceos2 – ceos3 | 10.0.23.0/31     |
| ceos1 – srl1  | 10.0.14.0/31     |
| ceos2 – srl1  | 10.0.24.0/31     |
| Loopback0     | 10.255.0.1–3/32 (cEOS) |

Login, SSH on the management address: `admin` / `admin` on the cEOS devices,
`admin` / `NokiaSrl1!` (the image default) on srl1.

## Requirements

- Docker, and containerlab. Your user must be in the `clab_admins` group, or
  set `CONTAINERLAB="sudo containerlab"`.
- The cEOS image `registry.opsmill.io/external/ceos-image:4.29.0.2F`. To use
  another image, change `topology.kinds.ceos.image` in `lab/sandbox.clab.yml`.
- The SR Linux image `ghcr.io/nokia/srlinux:24.10.1`, public on GitHub
  Container Registry (`topology.kinds.nokia_srlinux.image`).
- [uv](https://docs.astral.sh/uv/). `uv sync` installs Nornir from a Git commit,
  so it needs access to github.com, and a pre-release of nornir-scrapli from PyPI
  (see
  [Nornir from Git and the nornir-scrapli pre-release](#nornir-from-git-and-the-nornir-scrapli-pre-release)).
- About 5 GB of free memory for the four devices.

## Start and stop the lab

```bash
uv sync                  # install the package and its dependencies in .venv
uv run invoke up         # deploy, then wait until every device accepts SSH logins
uv run invoke validate   # check links and LLDP neighbors against inventory/hosts.yaml
uv run invoke backup     # save running configurations under backups/
uv run invoke down       # destroy the lab
```

The cEOS devices accept logins about 30 seconds after deploy, srl1 about one
minute after. A globally installed
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

`access`, `command`, `backup` and `validate` take `--hosts ceos1,ceos2` to work
on some devices only (comma-separated for invoke, repeated `--hosts` for the
scripts). `backup` and `validate` send EOS commands, so they work on the cEOS
devices only; `access` and `command` work on every device.

## Scripts

The invoke tasks call these scripts. You can also run them directly with
`uv run python scripts/<type>/<script>.py --help`. The scripts are
[cyclopts](https://github.com/BrianPugh/cyclopts) apps: the options come from
the parameters of the `main()` function and their help from its docstring.
`main()` is a coroutine; cyclopts runs it on an asyncio event loop.
Every script that connects to devices takes `--hosts` (repeat it for several
hosts, for example `--hosts ceos1 --hosts ceos2`) and exits with 1 when a task
fails. The scripts that send EOS commands (`backup/`, `validate/` and
`runner/`) run on the cEOS devices only and reject `--hosts srl1`. Most also take `--verbose`, which prints the traceback of failed tasks.
Nornir also writes a log to `nornir.log`.

- `scripts/backup/backup_configs.py`: saves each running configuration to
  `backups/<YYYYmmdd-HHMMSS>/<host>.cfg` and points `backups/latest` at the
  newest run. Compare two runs with `diff -r backups/<older run> backups/latest`.
- `scripts/execute/run_commands.py`: runs show commands and prints the output.
- `scripts/troubleshoot/check_access.py`: checks, for each device, that the SSH
  port answers and that the login reaches the CLI prompt, and reports which
  step failed. `--wait N` retries failed devices for N seconds.
- `scripts/troubleshoot/show_inventory.py`: prints the hosts, addresses,
  platform and expected links that the scripts use.
- `scripts/validate/validate_lab.py`: checks that every lab link interface has
  line protocol up and that LLDP neighbors match the links in the inventory.
- `scripts/runner/`: scripts to explore how runs stream progress, cancel and
  time out. See [Streaming, cancellation and deadlines](#streaming-cancellation-and-deadlines).
- `scripts/operations/`: scripts that run the same Operation on the cEOS and
  SR Linux devices, sync and async. See [Operations](#operations).

## Streaming, cancellation and deadlines

The scripts in `scripts/runner/` run against the cEOS devices with the runner
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

## Operations

An Operation defines one action, such as "read the facts of the device", with a
typed input and a typed output, and no device commands. A task for each platform
implements it. `nr.run_operation(GetFacts)` runs, on each host, the task
registered for the host's platform, and each result holds an instance of the
output model, which mypy knows. The calling code has no condition on
`host.platform`. Operations come from
[nornir-automation/nornir#1115](https://github.com/nornir-automation/nornir/pull/1115)
(branch [`dga/feat-operation-s0e5v`](https://github.com/nornir-automation/nornir/tree/dga/feat-operation-s0e5v)).

The sandbox defines five Operations in `src/nornir_sandbox/operations/contracts.py`:

| Operation                    | Input                         | Output                                           |
|------------------------------|-------------------------------|--------------------------------------------------|
| `get_facts`                  | none                          | `Facts`: hostname, vendor, model, OS version, serial number |
| `get_config`                 | none                          | `Config`: running configuration as CLI text      |
| `get_lldp_neighbors`         | none                          | `LldpNeighbors`: local interface, neighbor name and port |
| `get_interfaces`             | optional `oper_state` filter  | `Interfaces`: name, description, administrative and operational state, MTU of each interface |
| `set_interface_descriptions` | description of each interface | `previous` description of each interface; `changed` and `diff` on the result |

The models of `get_interfaces` are Pydantic models; the models of the other
Operations are frozen dataclasses. Nornir accepts both:

- Nornir builds a Pydantic input model with `model_validate()`, so a mapping
  such as `{"oper_state": "up"}` is checked against the field types.
  `GetInterfacesInput` forbids unknown fields, so a misspelled field fails too.
  `run_operation()` and `arun_operation()` raise `OperationInputError`, with
  Pydantic's message, before any host connects.
- Nornir checks that each host's task returns an instance of the output model.
  A Pydantic output model also checks its own fields when the task builds it:
  `Interface` accepts only `"up"` and `"down"` as states and a positive MTU, so
  a parse function that maps a state wrongly fails the host with a
  `ValidationError` that names the field.
- A Pydantic result converts to JSON with `model_dump_json()` and back with
  `model_validate_json()`.

mypy checks the arguments of the Pydantic models with the `pydantic.mypy`
plugin (`[tool.pydantic-mypy]` in `pyproject.toml`).

`operations/eos.py` and `operations/srlinux.py` implement each Operation for
`arista_eos` and `nokia_srlinux`, twice: an `async def` task for the asyncio
runner and a `def` task for the threaded and serial runners. The functions
that turn the device JSON into the output models are in `operations/parse.py`,
with unit tests.

`pyproject.toml` registers the Operations and their tasks as entry points
(`nornir.plugins.operations` and `nornir.plugins.operation_tasks`). Nornir reads
them the first time a script runs an Operation; run `uv sync` after you change
them. A task is registered for one `<operation>.<platform>.<connection>` key,
and a key holds one task, either sync or async. Both forms use the `scrapli`
connection, so the sandbox registers the async tasks under the connection name
`scrapli` and the sync tasks under `scrapli_sync`. `init_nornir()` and
`init_nornir_sync()` set `core.operation_connection` to the name that matches
the runner, so the scripts do not pass `connection=`.

A program can also register a task in code, without an entry point, with
`OperationRegistry.register()` from `nornir.core.operations`. The `get_config`
tasks are registered this way: `pyproject.toml` has no task entry points for
`get_config`, and `scripts/operations/backup_configs_*.py` register the tasks
they need before they run the Operation:

```python
from nornir.core.operations import OperationRegistry

from nornir_sandbox.operations import ASYNC_CONNECTION, GetConfig, eos, srlinux

for platform, task in (("arista_eos", eos.get_config), ("nokia_srlinux", srlinux.get_config)):
    OperationRegistry.register(operation=GetConfig.name, platform=platform, connection=ASYNC_CONNECTION, task=task)
```

Registering the same task again for the same key does nothing. Registering a
different task for a key that already has one raises
`OperationAlreadyRegisteredError`, unless you pass `replace=True`. The
`GetConfig` class registers the Operation itself when it is defined, so the
Operation needs no entry point either; the sandbox keeps one so that
`list_operations.py` lists it. `list_operations.py` shows no tasks for
`get_config`, because it does not register them.

Each script in `scripts/operations/` has a sync form (`*_sync.py`,
`nr.run_operation()`, `--runner threaded` or `serial`) and an async form
(`*_async.py`, `nr.arun_operation()`). Both read the results with
`stream.results()` as they arrive.

- `get_facts_*.py`: prints the vendor, model, OS version and serial number of
  every device.
- `backup_configs_*.py`: saves the running configuration of every device under
  `backups/`, as `scripts/backup/backup_configs.py` does for the cEOS devices.
  They register the `get_config` tasks in code before they run the Operation.
- `validate_lldp_*.py`: checks the LLDP neighbors of every device against the
  links in `inventory/hosts.yaml`, with one function for both platforms.
- `set_descriptions_*.py`: sets the description of every link interface from
  `--template` (default `to {peer} {peer_interface}`, the descriptions of the
  startup configurations). The input of each host is a function of the host,
  built from its links. Each host reports `changed` and a diff; a second run
  with the same template reports no change. A host fails, without any change,
  when one of its interfaces does not exist.
- `get_interfaces_*.py`: prints the state, MTU and description of the
  interfaces of every device. `--params` takes the input of the Operation as
  JSON and passes it to Nornir unchanged, so Pydantic validates it. `--json`
  prints each result with `model_dump_json()`.
- `list_operations.py`: prints the Operations, their models and their tasks,
  without connecting to any device.

```bash
uv run python scripts/operations/get_facts_async.py
uv run python scripts/operations/get_facts_sync.py --runner serial
uv run python scripts/operations/set_descriptions_async.py --template "link to {peer} {peer_interface}"
uv run python scripts/operations/set_descriptions_sync.py     # restore the startup descriptions
uv run python scripts/operations/get_interfaces_async.py --params '{"oper_state": "up"}'
uv run python scripts/operations/get_interfaces_sync.py --json
uv run python scripts/operations/list_operations.py
```

A host whose task fails, or whose platform has no task registered, fails on its
own (`OperationNotRegisteredError` for a missing task); the other hosts
continue.

## Repository layout

```text
inventory/                  Nornir inventory (SimpleInventory)
  hosts.yaml                devices: management address, group, expected links
  groups.yaml               platform and scrapli connection options
  defaults.yaml             username and password
lab/
  sandbox.clab.yml          containerlab topology
  configs/ceosN.cfg         startup configuration of each cEOS device
  configs/srl1.cli          SR Linux commands applied on top of its default configuration
scripts/                    command-line scripts, one directory per type
  backup/ execute/ troubleshoot/ validate/
  runner/                   streaming, cancellation and deadline examples, sync and async
  operations/               Operation examples on cEOS and SR Linux, sync and async
src/nornir_sandbox/         shared Python package
  core.py                   init_nornir() and init_nornir_sync(): Nornir object for the lab (runner, time limits), with host and platform filters
  eos.py                    send(), send_json(), configure() and their sync versions, to run commands on EOS
  srlinux.py                the same functions for SR Linux
  operations/               Operations: contracts, EOS and SR Linux tasks, parse functions
  backups.py                backup directories under backups/
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
- `platform="arista_eos"` of `init_nornir()` and `init_nornir_sync()` keeps only
  the cEOS hosts, and raises `ValueError` when `hosts` names a host of another
  platform.

### Nornir from Git and the nornir-scrapli pre-release

The asyncio runner, `AInitNornir`, and the streaming, cancellation and time
limits of the runners are in Nornir 3.7.0a1. The Nornir guide
[Testing the 3.7 pre-releases](https://github.com/nornir-automation/nornir/blob/v3.7.0a1/docs/upgrading/testing_3_7_prereleases.rst)
lists what changed.

Operations
([nornir-automation/nornir#1115](https://github.com/nornir-automation/nornir/pull/1115))
are not released. `pyproject.toml` installs Nornir from a commit of the branch
[`dga/feat-operation-s0e5v`](https://github.com/nornir-automation/nornir/tree/dga/feat-operation-s0e5v)
under `[tool.uv.sources]`. That branch holds the code of Nornir 3.7.0a1 plus
Operations, but its version is still 3.6.1.

The asyncio scrapli connection and tasks are in nornir-scrapli 1.6.0a1, a
pre-release on PyPI that requires Nornir 3.7.0a1 or later. `pyproject.toml` pins
the exact version, because uv and pip install a pre-release only when the
version is given. Because the Nornir commit reports 3.6.1,
`override-dependencies` under `[tool.uv]` replaces nornir-scrapli's Nornir
requirement with the same commit. When you move to another Nornir commit,
change the Nornir revision in both places and run `uv lock`.

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
- A new Operation: define its models, as dataclasses or Pydantic models, and its
  class in `src/nornir_sandbox/operations/contracts.py`, write an `async def` and a `def`
  task in `operations/eos.py` and `operations/srlinux.py`, put the parsing of
  the device output in `operations/parse.py` with a unit test, and register the
  class and the four tasks in `pyproject.toml`, or register the tasks in code
  with `OperationRegistry.register()` as the backup scripts do for `get_config`. Run `uv sync`, then
  `scripts/operations/list_operations.py` to check the registration.
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
- The devices see each other as LLDP neighbors on `Management0` (`mgmt0` on
  SR Linux) because they share the management network. The LLDP checks ignore
  management interfaces.
- Containerlab applies `lab/configs/srl1.cli` on top of the SR Linux default
  configuration, only when `lab/clab-sandbox/srl1/` does not exist.
- The SR Linux CLI asks the terminal for the cursor position when the session
  has no terminal size. scrapli does not answer, so the asyncio connection
  timed out getting the prompt. `inventory/groups.yaml` sets
  `transport_options.asyncssh.term_size` for the SR Linux hosts to avoid it.
- scrapli does not treat the SR Linux `Parsing error` and `Error:` messages as
  failures. `nornir_sandbox.srlinux` passes them in `failed_when_contains`.
- After `commit now`, SR Linux leaves candidate mode but scrapli still
  considers the session in configuration mode, so the next `send_configs()`
  sends `set` commands in running mode, which fail.
  `nornir_sandbox.srlinux.configure()` uses `commit stay` instead.
