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
- [uv](https://docs.astral.sh/uv/). `uv sync` installs Nornir and nornir-scrapli
  from Git commits, so it needs access to github.com (see
  [Unreleased Nornir and nornir-scrapli](#unreleased-nornir-and-nornir-scrapli)).
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

## Repository layout

```text
inventory/                  Nornir inventory (SimpleInventory)
  hosts.yaml                devices: management address, group, expected links
  groups.yaml               platform and scrapli connection options
  defaults.yaml             username and password
lab/
  sandbox.clab.yml          containerlab topology
  configs/ceosN.cfg         startup configuration of each device
scripts/                    command-line scripts, one directory per type
  backup/ execute/ troubleshoot/ validate/
src/nornir_sandbox/         shared Python package
  core.py                   init_nornir(): Nornir object for the lab (asyncio runner), with a host filter
  tasks/                    Nornir coroutine tasks (backup, access check, validation, EOS helpers)
  checks.py                 compares device output with the expected links; no device access
  cli.py                    --hosts and --verbose options and result summary shared by the scripts
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
connection. Each task is a coroutine, and all hosts run on one event loop in
the script's process, with no threads. Up to 10 hosts run a task at the same
time (`num_workers` of `init_nornir()`).

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
- Open the Nornir object with `async with init_nornir(...) as nr:`. On exit, it
  closes the connections of every host, failed hosts included, before the
  event loop stops.
- Code that blocks, such as file writes or `time.sleep()`, stops every host
  while it runs. Use `await asyncio.to_thread(...)` or `await asyncio.sleep()`,
  as in `tasks/backup.py` and `scripts/troubleshoot/check_access.py`.

### Unreleased Nornir and nornir-scrapli

The asyncio runner
([nornir-automation/nornir#1111](https://github.com/nornir-automation/nornir/pull/1111))
and the asyncio scrapli connection and tasks
([scrapli/nornir_scrapli#179](https://github.com/scrapli/nornir_scrapli/pull/179))
are not released. `pyproject.toml` installs both from Git commits under
`[tool.uv.sources]`. nornir-scrapli requires the same Nornir commit, so when
you move to newer commits, change both `rev` values together and run
`uv lock`.

## Add your own code

- A new script: add a file under the matching `scripts/<type>/` directory (or a
  new type directory). Start from `scripts/validate/validate_lab.py`: a
  cyclopts `App`, an `async def main()` function using the `Hosts` and
  `Verbose` option types from `nornir_sandbox.cli`,
  `async with init_nornir(hosts=...) as nr:`, `await nr.arun(...)`, and
  `print_summary(...)` for the output and exit code.
- A new Nornir task: add it as an `async def` function to
  `src/nornir_sandbox/tasks/`. Use `await send()` or `await send_json()` from
  `tasks/eos.py` to run commands on EOS. Put logic that does not need a device
  in a plain function, as in `checks.py`, so it can be unit tested.
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
