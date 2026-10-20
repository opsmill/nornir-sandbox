from __future__ import annotations

import asyncio
import time

from cyclopts import App
from nornir.core.task import Result, Task

from nornir_sandbox import close_failed_connections, init_nornir
from nornir_sandbox.cli import Hosts, Verbose, print_summary
from nornir_sandbox.eos import send_json

app = App()


async def check_access(task: Task, port: int = 22, timeout: float = 3.0) -> Result:
    """Check, in order, that the SSH port answers, that login works and that EOS runs commands.

    Failures are returned as a failed Result with the step that failed, instead
    of a traceback, so the output says where to look.
    """
    hostname = task.host.hostname or task.host.name
    try:
        _, writer = await asyncio.wait_for(asyncio.open_connection(hostname, port), timeout=timeout)
        writer.close()
        await writer.wait_closed()
    except (OSError, TimeoutError) as exc:
        reason = str(exc) or f"no answer after {timeout}s"
        return Result(host=task.host, failed=True, result=f"{hostname}:{port} not reachable: {reason}")

    try:
        version = await send_json(task, "show version")
    except Exception as exc:
        # A failed login leaves no connection behind, so the next attempt starts clean.
        return Result(host=task.host, failed=True, result=f"SSH login or command failed: {exc}")

    uptime = int(float(version.get("uptime", 0)))
    return Result(
        host=task.host,
        result=f"{version.get('modelName')} EOS {version.get('version')}, up {uptime}s",
    )


@app.default
async def main(*, hosts: Hosts = None, wait: int = 0, interval: int = 10, verbose: Verbose = False) -> int:
    """Check that each lab device answers on SSH, accepts the login and runs commands.

    cEOS accepts SSH logins about 30 seconds after `containerlab deploy`.

    Parameters
    ----------
    wait
        Seconds to keep retrying the hosts that fail.
    interval
        Seconds between retries.
    """
    nr = await init_nornir(hosts=hosts)
    async with nr:
        deadline = time.monotonic() + wait
        pending = set(nr.inventory.hosts)
        passed = {}

        while True:
            targets = nr.filter(filter_func=lambda host, names=frozenset(pending): host.name in names)
            result = await targets.arun(task=check_access, on_failed=True)
            passed.update({host: multi for host, multi in result.items() if not multi.failed})
            pending = {host for host, multi in result.items() if multi.failed}
            if not pending or time.monotonic() >= deadline:
                break
            await close_failed_connections(nr, result)
            print(f"waiting for {', '.join(sorted(pending))} ({int(deadline - time.monotonic())}s left)", flush=True)
            await asyncio.sleep(interval)

    # Hosts that passed in an earlier round are reported with their passing result.
    result.update(passed)
    return print_summary([result], verbose=verbose)


if __name__ == "__main__":
    app()
