"""Start a run in the background with nr.asubmit(), and cancel it when a host fails.

`await nr.asubmit()` returns an AsyncRunHandle at once; the hosts run as asyncio tasks
on the script's event loop. `async for result in handle.results()` yields the final
Result of each host in the order the hosts finish. On the first failed host, the
script calls handle.cancel(): the asyncio runner interrupts each running host at the
call it is awaiting, for example a command waiting for the device, and hosts that have
not started never start.

--watch starts a second asyncio task that prints handle.progress(), the latest partial
result of each host, every half second.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncGenerator, Collection

from cyclopts import App
from nornir.core.run import AsyncRunHandle
from nornir.core.task import PartialResult, Result, Task

from nornir_sandbox import init_nornir
from nornir_sandbox.checks import summarize_fact
from nornir_sandbox.cli import EosHosts, FailHosts, SlowHosts, Verbose, format_event, print_summary
from nornir_sandbox.eos import PLATFORM, send, send_json, sleep_command

app = App()

COMMANDS = ("show version", "show interfaces", "show lldp neighbors")
# EOS rejects this command: the lab devices have no Ethernet99.
REJECTED_COMMAND = "show interfaces Ethernet99"


async def collect_facts(
    task: Task, delay: float, slow_hosts: Collection[str], slow_delay: float, fail_hosts: Collection[str]
) -> AsyncGenerator[PartialResult | Result, None]:
    """Run COMMANDS one by one, and yield a PartialResult before and after each.

    An async generator cannot `return` a value, so the task ends by yielding its
    final Result. After handle.cancel(), a running host stops at the call it is
    awaiting, for example during the wait on the device. A host in `fail_hosts` sends
    REJECTED_COMMAND before its second command, which raises EOSCommandError and fails
    the task.
    """
    wait = slow_delay if task.host.name in slow_hosts else delay
    facts = []
    for index, command in enumerate(COMMANDS):
        if wait:
            yield PartialResult(task.host, f"waiting {wait:g}s on the device", progress=index / len(COMMANDS))
            await send(task, sleep_command(wait))
        if index == 1 and task.host.name in fail_hosts:
            await send(task, REJECTED_COMMAND)
        facts.append(summarize_fact(command, await send_json(task, command)))
        yield PartialResult(task.host, f"{command}: {facts[-1]}", progress=(index + 1) / len(COMMANDS))
    yield Result(host=task.host, result=", ".join(facts))


async def watch_progress(handle: AsyncRunHandle, started_at: float) -> None:
    """Print the latest partial result of each host every half second, until the run is done."""
    while not handle.done:
        snapshot = ", ".join(
            f"{host}={partial.progress:.0%}" if partial.progress is not None else f"{host}={partial.result}"
            for host, partial in sorted(handle.progress().items())
        )
        print(f"{time.monotonic() - started_at:6.2f}s  [watch] {snapshot or 'no progress yet'}")
        await asyncio.sleep(0.5)


@app.default
async def main(
    *,
    hosts: EosHosts = None,
    workers: int = 10,
    delay: float = 1.0,
    slow_host: SlowHosts = None,
    slow_delay: float = 3.0,
    fail_host: FailHosts = None,
    keep_going: bool = False,
    watch: bool = False,
    verbose: Verbose = False,
) -> int:
    """Run a fact-collection task in the background, and cancel the run on the first failed host.

    Example: cancel_on_failure_async.py --fail-host ceos2 --slow-host ceos3 --watch

    Parameters
    ----------
    workers
        Number of hosts that run at the same time. With 1, the hosts after the failed
        one never start.
    delay
        Seconds each host waits on the device before each command.
    slow_delay
        Seconds a --slow-host waits on the device before each command.
    keep_going
        Do not cancel the run when a host fails.
    watch
        Print handle.progress() every half second from another asyncio task.
    """
    nr = await init_nornir(hosts=hosts, platform=PLATFORM, num_workers=workers)
    async with nr:
        started_at = time.monotonic()
        handle = await nr.asubmit(
            task=collect_facts,
            delay=delay,
            slow_hosts=frozenset(slow_host or ()),
            slow_delay=slow_delay,
            fail_hosts=frozenset(fail_host or ()),
        )
        print(f"asubmit() returned run {handle.id} after {time.monotonic() - started_at:.3f}s; done={handle.done}\n")
        watcher = asyncio.create_task(watch_progress(handle, started_at)) if watch else None

        async for result in handle.results():
            print(format_event(result, started_at))
            if result.failed and not keep_going:
                print(f"{time.monotonic() - started_at:6.2f}s  calling handle.cancel()")
                handle.cancel()
                break

        aggregated = await handle.result()
        print(f"\nhandle.result() returned after {time.monotonic() - started_at:.2f}s; done={handle.done}")
        if watcher is not None:
            await watcher
        print(f"nr.data.failed_hosts: {sorted(nr.data.failed_hosts)}\n")
    return print_summary([aggregated], verbose)


if __name__ == "__main__":
    app()
