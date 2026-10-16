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

- Scripts are a cyclopts `App` with one `@app.default` `main()` function. Use
  the `Hosts` and `Verbose` types and `print_summary()` from
  `nornir_sandbox.cli`, and return 1 when a task fails.
- Device access goes through the scrapli connection (`tasks/eos.py`). Parse EOS
  output with `| json`, not text parsing.
- The project uses scrapli v2 and `nornir-scrapli` from
  https://github.com/scrapli/nornir_scrapli/pull/176 (pinned in `pyproject.toml`
  under `[tool.uv.sources]`). Use the v2 API: `Cli.send_input()`, nested
  connection `extras` (`auth`, `session`, `transport`, `cli`), and v2 task
  arguments such as `requested_mode`. Results have `failed` but no
  `raise_for_status()`.
- Logic that does not need a device goes in plain functions with unit tests
  (`checks.py`).
