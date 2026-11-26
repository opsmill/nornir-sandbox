"""Give each host, and the whole run, a deadline on the threaded or serial runner.

A host that passes its per-host timeout, or is still running or waiting when the run
timeout passes, is recorded with TaskTimeoutError; the other hosts keep their results.
The deadlines are set on the runner (ThreadedRunner(per_host_timeout=..., timeout=...)),
or with --deadlines-on run, for this run only with nr.stream(run_options=RunOptions(...)).

The task (collect_facts, below) waits on the device before each command (`bash sleep`),
in a call that blocks the thread:

- The threaded runner records a host as timed out when its deadline passes, even while
  the host's thread is blocked, and the run ends on time. The blocked thread keeps
  running until the call returns and the task reaches its next checkpoint; the script
  prints how long that takes. --split-wait splits the device wait into shorter calls
  with task.checkpoint() between them, so the thread stops sooner.
- The serial runner checks deadlines at checkpoints only, so a host is recorded as
  timed out only when its blocking call returns.
"""

from __future__ import annotations

import math
import time
from collections.abc import Collection, Generator
from typing import Annotated, Literal

from cyclopts import App, Parameter
from nornir.core.exceptions import TaskTimeoutError
from nornir.core.run import RunOptions
from nornir.core.task import PartialResult, Result, Task

from nornir_sandbox import init_nornir_sync
from nornir_sandbox.checks import summarize_fact
from nornir_sandbox.cli import EosHosts, SlowHosts, SyncRunner, Verbose, format_event, print_summary
from nornir_sandbox.eos import PLATFORM, send_json_sync, send_sync, sleep_command

app = App()

DeadlinesOn = Annotated[
    Literal["runner", "run"],
    Parameter(help="Set the deadlines on the runner, or for this run only with RunOptions."),
]

COMMANDS = ("show version", "show interfaces", "show lldp neighbors")

# Host name -> time.monotonic() when its task function stopped running, written by the worker threads.
STOPPED_AT: dict[str, float] = {}


def collect_facts(
    task: Task, delay: float, slow_hosts: Collection[str], slow_delay: float, wait_step: float | None
) -> Generator[PartialResult, None, Result[str]]:
    """Run COMMANDS one by one, and yield a PartialResult before and after each.

    Each `yield` and each task.checkpoint() is a checkpoint: a timed-out host stops
    there. The wait on the device blocks the thread until it ends; with `wait_step`,
    the wait is split into calls of at most that many seconds, with a checkpoint
    before each. The `finally` block records when the function stops, on its thread.
    """
    wait = slow_delay if task.host.name in slow_hosts else delay
    calls = 1 if wait_step is None else max(1, math.ceil(wait / wait_step))
    facts = []
    try:
        for index, command in enumerate(COMMANDS):
            if wait:
                yield PartialResult(task.host, f"waiting {wait:g}s on the device", progress=index / len(COMMANDS))
                for _ in range(calls):
                    task.checkpoint()
                    send_sync(task, sleep_command(wait / calls))
            facts.append(summarize_fact(command, send_json_sync(task, command)))
            yield PartialResult(task.host, f"{command}: {facts[-1]}", progress=(index + 1) / len(COMMANDS))
        return Result(host=task.host, result=", ".join(facts))
    finally:
        STOPPED_AT[task.host.name] = time.monotonic()


@app.default
def main(
    *,
    hosts: EosHosts = None,
    runner: SyncRunner = "threaded",
    workers: int = 10,
    delay: float = 0.5,
    slow_host: SlowHosts = None,
    slow_delay: float = 10.0,
    per_host_timeout: float | None = 4.0,
    run_timeout: float | None = None,
    deadlines_on: DeadlinesOn = "runner",
    split_wait: float | None = None,
    verbose: Verbose = False,
) -> int:
    """Run a fact-collection task with deadlines, and print when each host is recorded.

    Example: deadlines_sync.py --slow-host ceos3 --per-host-timeout 4

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
    split_wait
        Split each device wait into calls of at most this many seconds, with
        task.checkpoint() before each.
    """
    on_runner = deadlines_on == "runner"
    run_options = None if on_runner else RunOptions(per_host_timeout=per_host_timeout, timeout=run_timeout)
    with init_nornir_sync(
        hosts=hosts,
        platform=PLATFORM,
        runner=runner,
        num_workers=workers,
        per_host_timeout=per_host_timeout if on_runner else None,
        timeout=run_timeout if on_runner else None,
    ) as nr:
        print(f"{type(nr.runner).__name__}.run_options = {getattr(nr.runner, 'run_options', None)}")
        print(f"run_options passed to nr.stream() = {run_options}\n")
        started_at = time.monotonic()
        stream = nr.stream(
            task=collect_facts,
            run_options=run_options,
            delay=delay,
            slow_hosts=frozenset(slow_host or ()),
            slow_delay=slow_delay,
            wait_step=split_wait,
        )
        for event in stream:
            print(format_event(event, started_at))
        result = stream.result()
        print(f"\nThe run returned after {time.monotonic() - started_at:.2f}s.")

        for host, multi in result.items():
            error = multi[0].exception
            if not isinstance(error, TaskTimeoutError):
                continue
            print(f"  {host}: TaskTimeoutError scope={error.scope} timeout={error.timeout:g}s started={error.started}")
            if not error.started:
                continue
            # On the threaded runner, the host's thread may still be blocked in a call to the device.
            while host not in STOPPED_AT:
                time.sleep(0.05)
            print(f"  {host}: its task function stopped at {STOPPED_AT[host] - started_at:.2f}s, on its thread")
    print()
    return print_summary([result], verbose)


if __name__ == "__main__":
    app()
