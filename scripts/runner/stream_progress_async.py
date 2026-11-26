"""Stream the progress of a run with nr.astream() on the asyncio runner.

The task (collect_facts, below) is an async generator: it
yields a PartialResult before and after each show command, and ends by yielding its
final Result. `async for event in nr.astream(...)` receives those partial results and
the final Result of each host as they happen. Leaving the loop early (--break-after)
cancels the hosts that are still running, at the call they are awaiting.

With --processor, the same task runs with plain `await nr.arun()`, and a processor
that defines task_instance_progress() prints the partial results instead. The results
that nr.arun() returns hold the final Result of each host only.
"""

from __future__ import annotations

import time
from collections.abc import AsyncGenerator, Collection
from typing import Any

from cyclopts import App
from nornir.core.inventory import Host
from nornir.core.task import AggregatedResult, MultiResult, PartialResult, Result, Task

from nornir_sandbox import init_nornir
from nornir_sandbox.checks import summarize_fact
from nornir_sandbox.cli import FailHosts, Hosts, SlowHosts, Verbose, format_event, print_summary
from nornir_sandbox.eos import send, send_json, sleep_command

app = App()

COMMANDS = ("show version", "show interfaces", "show lldp neighbors")
# EOS rejects this command: the lab devices have no Ethernet99.
REJECTED_COMMAND = "show interfaces Ethernet99"


async def collect_facts(
    task: Task, delay: float, slow_hosts: Collection[str], slow_delay: float, fail_hosts: Collection[str]
) -> AsyncGenerator[PartialResult | Result, None]:
    """Run COMMANDS one by one, and yield a PartialResult before and after each.

    An async generator cannot `return` a value, so the task ends by yielding its
    final Result. A cancelled host stops at the call it is awaiting, for example
    during the wait on the device. A host in `fail_hosts` sends REJECTED_COMMAND
    before its second command, which raises EOSCommandError and fails the task.
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


class PrintProgress:
    """Processor that prints each partial result and each host's final result."""

    def __init__(self, started_at: float) -> None:
        self.started_at = started_at

    def task_instance_progress(self, task: Task, host: Host, partial: PartialResult) -> None:
        print(format_event(partial, self.started_at))

    def task_started(self, task: Task) -> None:
        pass

    def task_completed(self, task: Task, result: AggregatedResult) -> None:
        pass

    def task_instance_started(self, task: Task, host: Host) -> None:
        pass

    def task_instance_completed(self, task: Task, host: Host, result: MultiResult) -> None:
        print(format_event(result[0], self.started_at))

    def subtask_instance_started(self, task: Task, host: Host) -> None:
        pass

    def subtask_instance_completed(self, task: Task, host: Host, result: MultiResult) -> None:
        pass


@app.default
async def main(
    *,
    hosts: Hosts = None,
    delay: float = 1.0,
    slow_host: SlowHosts = None,
    slow_delay: float = 3.0,
    fail_host: FailHosts = None,
    break_after: int | None = None,
    processor: bool = False,
    verbose: Verbose = False,
) -> int:
    """Run a fact-collection task that reports progress, and print each event as it arrives.

    Example: stream_progress_async.py --slow-host ceos3 --fail-host ceos2 --break-after 4

    Parameters
    ----------
    delay
        Seconds each host waits on the device before each command.
    slow_delay
        Seconds a --slow-host waits on the device before each command.
    break_after
        Leave the loop after this many events, which cancels the run.
    processor
        Run with nr.arun() and print the partial results from a processor, instead of
        iterating over nr.astream().
    """
    task_args: dict[str, Any] = {
        "delay": delay,
        "slow_hosts": frozenset(slow_host or ()),
        "slow_delay": slow_delay,
        "fail_hosts": frozenset(fail_host or ()),
    }
    nr = await init_nornir(hosts=hosts)
    async with nr:
        started_at = time.monotonic()
        if processor:
            result = await nr.with_processors([PrintProgress(started_at)]).arun(task=collect_facts, **task_args)
            print("\nnr.arun() returned. Types stored for each host:")
            for host, multi in result.items():
                print(f"  {host}: {[type(item).__name__ for item in multi]}")
        else:
            # `async with` cancels the run on exit if it has not finished. Leaving the
            # `async for` loop early cancels it too, when the loop's iterator is dropped.
            async with nr.astream(task=collect_facts, **task_args) as stream:
                print(f"astream() returned an {type(stream).__name__}; the run starts on the event loop")
                print(f"{'elapsed':>7}  {'host':<6} event\n")
                count = 0
                async for event in stream:
                    count += 1
                    print(format_event(event, started_at))
                    if break_after is not None and count >= break_after:
                        print(f"\nLeaving the loop after {count} events: the run is cancelled.")
                        break
            result = await stream.result()
            print(f"\nstream.result() returned after {time.monotonic() - started_at:.2f}s")
    print()
    return print_summary([result], verbose)


if __name__ == "__main__":
    app()
