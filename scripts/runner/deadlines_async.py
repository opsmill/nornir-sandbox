"""Give each host, and the whole run, a deadline on the asyncio runner.

A host that passes its per-host timeout, or is still running or waiting when the run
timeout passes, is recorded with TaskTimeoutError; the other hosts keep their results.
The deadlines are set on the runner (AsyncioRunner(per_host_timeout=..., timeout=...)),
or with --deadlines-on run, for this run only with nr.astream(run_options=RunOptions(...)).

The task (collect_facts, below) waits on the device before each command (`bash sleep`).
The asyncio runner cancels a timed-out host at the call it is awaiting, so the host
stops at its deadline, in the middle of the device wait, without checkpoints in the
task. The script prints when each timed-out task function stopped, to compare with
deadlines_sync.py.
"""

from __future__ import annotations

import time
from collections.abc import AsyncGenerator, Collection
from typing import Annotated, Literal

from cyclopts import App, Parameter
from nornir.core.exceptions import TaskTimeoutError
from nornir.core.run import RunOptions
from nornir.core.task import PartialResult, Result, Task

from nornir_sandbox import init_nornir
from nornir_sandbox.checks import summarize_fact
from nornir_sandbox.cli import EosHosts, SlowHosts, Verbose, format_event, print_summary
from nornir_sandbox.eos import PLATFORM, send, send_json, sleep_command

app = App()

DeadlinesOn = Annotated[
    Literal["runner", "run"],
    Parameter(help="Set the deadlines on the runner, or for this run only with RunOptions."),
]

COMMANDS = ("show version", "show interfaces", "show lldp neighbors")

# Host name -> time.monotonic() when its task function stopped running.
STOPPED_AT: dict[str, float] = {}


async def collect_facts(
    task: Task, delay: float, slow_hosts: Collection[str], slow_delay: float
) -> AsyncGenerator[PartialResult | Result[str], None]:
    """Run COMMANDS one by one, and yield a PartialResult before and after each.

    An async generator cannot `return` a value, so the task ends by yielding its
    final Result. A timed-out host stops at the call it is awaiting, for example
    during the wait on the device. The `finally` block records when the function stops.
    """
    wait = slow_delay if task.host.name in slow_hosts else delay
    facts = []
    try:
        for index, command in enumerate(COMMANDS):
            if wait:
                yield PartialResult(task.host, f"waiting {wait:g}s on the device", progress=index / len(COMMANDS))
                await send(task, sleep_command(wait))
            facts.append(summarize_fact(command, await send_json(task, command)))
            yield PartialResult(task.host, f"{command}: {facts[-1]}", progress=(index + 1) / len(COMMANDS))
        yield Result(host=task.host, result=", ".join(facts))
    finally:
        STOPPED_AT[task.host.name] = time.monotonic()


@app.default
async def main(
    *,
    hosts: EosHosts = None,
    workers: int = 10,
    delay: float = 0.5,
    slow_host: SlowHosts = None,
    slow_delay: float = 10.0,
    per_host_timeout: float | None = 4.0,
    run_timeout: float | None = None,
    deadlines_on: DeadlinesOn = "runner",
    verbose: Verbose = False,
) -> int:
    """Run a fact-collection task with deadlines, and print when each host is recorded.

    Example: deadlines_async.py --slow-host ceos3 --per-host-timeout 4

    Parameters
    ----------
    workers
        Number of hosts that run at the same time. With 1 and --run-timeout, the hosts
        that have not started when the run timeout passes are recorded with started=False.
    delay
        Seconds each host waits on the device before each command.
    slow_delay
        Seconds a --slow-host waits on the device before each command.
    per_host_timeout
        Seconds each host may run, from the moment it starts.
    run_timeout
        Seconds the whole run may take.
    """
    on_runner = deadlines_on == "runner"
    run_options = None if on_runner else RunOptions(per_host_timeout=per_host_timeout, timeout=run_timeout)
    nr = await init_nornir(
        hosts=hosts,
        platform=PLATFORM,
        num_workers=workers,
        per_host_timeout=per_host_timeout if on_runner else None,
        timeout=run_timeout if on_runner else None,
    )
    async with nr:
        print(f"{type(nr.runner).__name__}.run_options = {getattr(nr.runner, 'run_options', None)}")
        print(f"run_options passed to astream() = {run_options}\n")
        started_at = time.monotonic()
        stream = nr.astream(
            task=collect_facts,
            run_options=run_options,
            delay=delay,
            slow_hosts=frozenset(slow_host or ()),
            slow_delay=slow_delay,
        )
        async for event in stream:
            print(format_event(event, started_at))
        result = await stream.result()
        print(f"\nThe run returned after {time.monotonic() - started_at:.2f}s.")

        for host, multi in result.items():
            error = multi[0].exception
            if not isinstance(error, TaskTimeoutError):
                continue
            print(f"  {host}: TaskTimeoutError scope={error.scope} timeout={error.timeout:g}s started={error.started}")
            if error.started:
                print(f"  {host}: its task function stopped at {STOPPED_AT[host] - started_at:.2f}s")
    print()
    return print_summary([result], verbose)


if __name__ == "__main__":
    app()
