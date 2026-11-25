"""Invoke tasks for the sandbox. Run `invoke --list` to see them.

Scripts and dev tools run through `uv run`, so the tasks work the same whether
invoke comes from the project environment (`uv run invoke`) or from a global
install (`inv`).

Lab tasks call containerlab. If your user is not in the clab_admins group, set
CONTAINERLAB="sudo containerlab".
"""

from __future__ import annotations

import os
import shlex
import sys
from pathlib import Path

from invoke.context import Context
from invoke.tasks import task

ROOT = Path(__file__).parent
TOPOLOGY = "lab/sandbox.clab.yml"
CONTAINERLAB = os.environ.get("CONTAINERLAB", "containerlab")
PYTHON = "uv run python"
RAY_COMPOSE = "docker compose -f lab/ray/compose.yml"
RAY_ADDRESS = "172.24.100.21:6379"
# Address of this machine on the lab network (the sandbox-mgmt bridge), used when it
# joins the Ray cluster so that scripts here can be Ray drivers.
RAY_DRIVER_IP = "172.24.100.1"


def _hosts_option(hosts: str) -> str:
    """Turn the comma-separated --hosts of the invoke tasks into the scripts' repeated --hosts."""
    return "".join(f" --hosts {shlex.quote(host)}" for host in hosts.split(",") if host)


@task(help={"wait": "seconds to wait for SSH logins on every device after deploy; 0 skips the wait"})
def up(c: Context, wait: int = 300) -> None:
    """Deploy the lab, then wait until every device accepts SSH logins."""
    with c.cd(ROOT):
        c.run(f"{CONTAINERLAB} deploy -t {TOPOLOGY}", pty=True)
        if wait:
            c.run(f"{PYTHON} scripts/troubleshoot/check_access.py --wait {wait}", pty=True)


@task(
    help={
        "keep_files": "keep lab/clab-sandbox/ so the next deploy boots from the configs saved on the devices "
        "instead of lab/configs/"
    }
)
def down(c: Context, keep_files: bool = False) -> None:
    """Destroy the lab containers and, unless --keep-files, the lab directory."""
    cleanup = "" if keep_files else " --cleanup"
    with c.cd(ROOT):
        c.run(f"{CONTAINERLAB} destroy -t {TOPOLOGY}{cleanup}", pty=True)


@task(help={"wait": "seconds to wait for SSH logins after deploy; 0 skips the wait"})
def restart(c: Context, wait: int = 300) -> None:
    """Destroy the lab and deploy it again from lab/configs/."""
    down(c)
    up(c, wait=wait)


@task
def status(c: Context) -> None:
    """Show the lab containers, their state and management addresses."""
    with c.cd(ROOT):
        c.run(f"{CONTAINERLAB} inspect -t {TOPOLOGY}", pty=True)


@task(name="ray-up")
def ray_up(c: Context) -> None:
    """Start the Ray cluster on the lab network and join this machine to it, with no CPUs.

    Run `invoke up` first: the cluster uses the lab's management network.
    """
    with c.cd(ROOT):
        c.run(f"{RAY_COMPOSE} up -d --build --wait", pty=True)
        c.run(
            f"uv run ray start --address={RAY_ADDRESS} --num-cpus=0 --node-ip-address={RAY_DRIVER_IP} "
            "--node-name=ray-driver",
            # Without a pseudo-terminal: closing it would hang up the raylet this starts.
            pty=False,
            env={"RAY_ENABLE_UV_RUN_RUNTIME_ENV": "0"},
        )
    print(f"\nRun the scripts with the Ray runner on this cluster:\n  export RAY_ADDRESS={RAY_ADDRESS}")


@task(name="ray-down")
def ray_down(c: Context) -> None:
    """Remove this machine from the Ray cluster, then stop the cluster's containers."""
    with c.cd(ROOT):
        c.run("uv run ray stop", pty=True, warn=True)
        c.run(f"{RAY_COMPOSE} down", pty=True)


@task(name="ray-status")
def ray_status(c: Context) -> None:
    """Show the Ray nodes and their resources, including the site resources."""
    with c.cd(ROOT):
        c.run(f"uv run ray status --address={RAY_ADDRESS}", pty=True)


@task(help={"wait": "seconds to keep retrying hosts that fail", "hosts": "comma-separated host names"})
def access(c: Context, wait: int = 0, hosts: str = "") -> None:
    """Check SSH reachability and login on the devices."""
    with c.cd(ROOT):
        c.run(f"{PYTHON} scripts/troubleshoot/check_access.py --wait {wait}{_hosts_option(hosts)}", pty=True)


@task
def inventory(c: Context) -> None:
    """Print the Nornir inventory from inventory/."""
    with c.cd(ROOT):
        c.run(f"{PYTHON} scripts/troubleshoot/show_inventory.py", pty=True)


@task(help={"hosts": "comma-separated host names"})
def backup(c: Context, hosts: str = "") -> None:
    """Save the running configuration of the devices under backups/."""
    with c.cd(ROOT):
        c.run(f"{PYTHON} scripts/backup/backup_configs.py{_hosts_option(hosts)}", pty=True)


@task(
    iterable=["check"],
    help={"check": "interfaces or lldp; repeat to run several (default: all)", "hosts": "comma-separated host names"},
)
def validate(c: Context, check: list[str] | None = None, hosts: str = "") -> None:
    """Check that the devices match the links in inventory/hosts.yaml."""
    checks = "".join(f" --check {name}" for name in check or [])
    with c.cd(ROOT):
        c.run(f"{PYTHON} scripts/validate/validate_lab.py{checks}{_hosts_option(hosts)}", pty=True)


@task(
    iterable=["command"],
    help={"command": "command to run; repeat to run several", "hosts": "comma-separated host names"},
)
def command(c: Context, command: list[str] | None = None, hosts: str = "") -> None:
    """Run show commands on the devices and print the output."""
    if not command:
        sys.exit('give at least one command, for example: invoke command -c "show version"')
    commands = "".join(f" {shlex.quote(cmd)}" for cmd in command)
    with c.cd(ROOT):
        c.run(f"{PYTHON} scripts/execute/run_commands.py{commands}{_hosts_option(hosts)}", pty=True)


@task
def lint(c: Context) -> None:
    """Run ruff and mypy without changing files."""
    with c.cd(ROOT):
        c.run("uv run ruff check .", pty=True)
        c.run("uv run ruff format --check .", pty=True)
        c.run("uv run mypy", pty=True)


@task(name="format")
def format_files(c: Context) -> None:
    """Format the code and apply ruff's automatic fixes."""
    with c.cd(ROOT):
        c.run("uv run ruff format .", pty=True)
        c.run("uv run ruff check --fix .", pty=True)


@task
def test(c: Context) -> None:
    """Run the unit tests. They do not need the lab."""
    with c.cd(ROOT):
        c.run("uv run pytest", pty=True)
