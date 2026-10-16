# AGENTS.md

Containerlab lab with three Arista cEOS devices, Nornir scripts to work with
them, and the shared `nornir_sandbox` package. See README.md for details.

## Key directories

- `inventory/`: static Nornir SimpleInventory (`hosts.yaml`, `groups.yaml`,
  `defaults.yaml`). Each host lists its lab links under `data.links`.
- `lab/`: containerlab topology (`sandbox.clab.yml`) and device startup
  configurations (`configs/`). Keep it in sync with `inventory/hosts.yaml`;
  `tests/test_inventory.py` checks that they match.
- `scripts/<type>/`: cyclopts command-line scripts grouped by type (backup,
  execute, troubleshoot, validate).
- `src/nornir_sandbox/`: shared code: `init_nornir()`, Nornir tasks, pure
  comparison functions, shared cyclopts option types.
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
  `nornir_sandbox.cli`, open Nornir with `async with init_nornir(...) as nr:`,
  run tasks with `await nr.arun(...)`, and return 1 when a task fails.
- Nornir uses the `asyncio` runner. Tasks are `async def` coroutines; use
  `nornir_scrapli.async_tasks`, not `nornir_scrapli.tasks`. Do not call blocking
  code (file I/O, `time.sleep`, sockets) in a task; use `asyncio.to_thread()` or
  the asyncio equivalent.
- Device access goes through the asyncio scrapli connection (`tasks/eos.py`).
  Parse EOS output with `| json`, not text parsing.
- nornir and nornir-scrapli come from unreleased Git commits pinned in
  `[tool.uv.sources]` in `pyproject.toml`; update both `rev` values together.
- Logic that does not need a device goes in plain functions with unit tests
  (`checks.py`).
