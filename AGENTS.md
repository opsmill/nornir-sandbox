# AGENTS.md

Containerlab lab with three Arista cEOS devices and one Nokia SR Linux device
(srl1), Nornir scripts to work with them, and the shared `nornir_sandbox`
package. See README.md for details.

## Key directories

- `inventory/`: static Nornir SimpleInventory (`hosts.yaml`, `groups.yaml`,
  `defaults.yaml`). Each host lists its lab links under `data.links`.
- `lab/`: containerlab topology (`sandbox.clab.yml`) and device startup
  configurations (`configs/`). Keep it in sync with `inventory/hosts.yaml`;
  `tests/test_inventory.py` checks that they match.
- `scripts/<type>/`: cyclopts command-line scripts grouped by type (backup,
  execute, troubleshoot, validate, runner, operations). `scripts/runner/` holds
  sync and async examples of run streaming, cancellation and deadlines.
  `scripts/operations/` holds sync and async examples of Operations on both
  platforms.
- `src/nornir_sandbox/`: shared code: `init_nornir()`, `init_nornir_sync()`,
  EOS and SR Linux command helpers (`eos.py`, `srlinux.py`), pure functions on
  device output (`checks.py`), shared cyclopts option types, and `operations/`
  (Operation contracts, EOS and SR Linux tasks, parse functions).
- `tests/`: unit tests; they do not need the lab.

## Commands

```bash
uv sync
uv run invoke up | down | restart | status    # lab lifecycle
uv run invoke access | backup | validate      # work with the devices
uv run invoke lint                            # ruff check, ruff format --check, mypy --strict
uv run invoke format
uv run invoke test
```

## Conventions

- Scripts are a cyclopts `App` with one `@app.default` `async def main()`
  function. Use the `Hosts` and `Verbose` types and `print_summary()` from
  `nornir_sandbox.cli`, create Nornir with `nr = await init_nornir(...)`, open
  it with `async with nr:`, run tasks with `await nr.arun(...)`, and return 1
  when a task fails.
- Nornir uses the `asyncio` runner. Tasks are `async def` coroutines; use
  `nornir_scrapli.async_tasks`, not `nornir_scrapli.tasks`. Do not call blocking
  code (file I/O, `time.sleep`, sockets) in a task; use `asyncio.to_thread()` or
  the asyncio equivalent.
- Exception: the `scripts/runner/*_sync.py` scripts show the threaded and serial
  runners. They have a plain `def main()`, open Nornir with
  `with init_nornir_sync(runner=...) as nr:`, and run `def` tasks that use
  `send_sync()`/`send_json_sync()`. Keep each runner example as a sync/async
  pair.
- A Nornir task lives in the script that uses it, above `main()`. Move it to
  the package only when several scripts use it.
- Device access goes through the scrapli connection (`eos.py`, `srlinux.py`).
  Parse EOS output with `| json` and SR Linux output with `| as json`, not text
  parsing.
- Scripts that send EOS commands use the `EosHosts` option type and pass
  `platform=PLATFORM` (from `nornir_sandbox.eos`) to `init_nornir()` or
  `init_nornir_sync()`, so they skip srl1. Scripts that work on every platform
  use `Hosts` and Operations.
- Operations: contracts in `operations/contracts.py`, with frozen dataclass
  models, or Pydantic models (`frozen=True`, `extra="forbid"`) as in
  `get_interfaces`; one `async def` and one
  `def` (`_sync`) task per platform in `operations/eos.py` and
  `operations/srlinux.py`, registered in `pyproject.toml` as
  `<operation>.<platform>.scrapli` (async) and `<operation>.<platform>.scrapli_sync`
  (sync). A registration key holds one task, hence the two connection names;
  `init_nornir()` and `init_nornir_sync()` set `core.operation_connection` for
  the runner. Run `uv sync` after changing entry points. Exception:
  `get_config` has no task entry points; `scripts/operations/backup_configs_*.py`
  register its tasks with `OperationRegistry.register()`. Keep each
  `scripts/operations/` example as a sync/async pair.
- `lab/sandbox.clab.yml`, `lab/configs/`, `inventory/hosts.yaml` and
  `inventory/groups.yaml` describe srl1 too; SR Linux interface names are
  `ethernet-1/N` in the inventory and `e1-N` in the topology.
- nornir-scrapli (1.6.0a1) is a PyPI pre-release pinned with `==` in
  `pyproject.toml`. nornir comes from a Git commit of the Operations branch
  (`dga/feat-operation-s0e5v`) pinned in `[tool.uv.sources]`; that commit
  reports version 3.6.1, so `[tool.uv] override-dependencies` repeats it to
  satisfy nornir-scrapli's `nornir>=3.7.0a1`. Change the nornir revision in
  both places.
- `init_nornir()` is a coroutine (`AInitNornir`). Outside the event loop, use
  `init_nornir_sync()`.
- The runner stops a host after `per_host_timeout` seconds and does not close
  its connection. Call `close_failed_connections(nr, result)` before running
  another task on the same hosts.
- Logic that does not need a device goes in plain functions with unit tests
  (`checks.py`).
