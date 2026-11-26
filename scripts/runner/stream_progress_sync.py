"""Stream the progress of a run with nr.stream() on the threaded or serial runner.

The task (collect_facts, below) is a generator: it yields a
PartialResult before and after each show command, and returns its final Result.
nr.stream() yields those partial results and the final Result of each host as they
happen. Leaving the loop early (--break-after) cancels the hosts that are still
running.

With --processor, the same task runs with plain nr.run(), and a processor that
defines task_instance_progress() prints the partial results instead. The results that
nr.run() returns hold the final Result of each host only.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Collection, Generator
from typing import Any

from cyclopts import App
from nornir.core.inventory import Host
from nornir.core.task import AggregatedResult, MultiResult, PartialResult, Result, Task

from nornir_sandbox import init_nornir_sync
from nornir_sandbox.checks import summarize_fact
from nornir_sandbox.cli import EosHosts, FailHosts, SlowHosts, SyncRunner, Verbose, format_event, print_summary
from nornir_sandbox.eos import PLATFORM, send_json_sync, send_sync, sleep_command

app = App()

COMMANDS = ("show version", "show interfaces", "show lldp neighbors")
# EOS rejects this command: the lab devices have no Ethernet99.
REJECTED_COMMAND = "show interfaces Ethernet99"


def collect_facts(
    task: Task, delay: float, slow_hosts: Collection[str], slow_delay: float, fail_hosts: Collection[str]
) -> Generator[PartialResult, None, Result[str]]:
    """Run COMMANDS one by one, and yield a PartialResult before and after each.

    Each `yield` is a checkpoint: a cancelled host stops there. The wait on the device
    blocks the thread until it ends. A host in `fail_hosts` sends REJECTED_COMMAND
    before its second command, which raises EOSCommandError and fails the task.
    """
    wait = slow_delay if task.host.name in slow_hosts else delay
    facts = []
    for index, command in enumerate(COMMANDS):
        if wait:
            yield PartialResult(task.host, f"waiting {wait:g}s on the device", progress=index / len(COMMANDS))
            send_sync(task, sleep_command(wait))
        if index == 1 and task.host.name in fail_hosts:
            send_sync(task, REJECTED_COMMAND)
        facts.append(summarize_fact(command, send_json_sync(task, command)))
        yield PartialResult(task.host, f"{command}: {facts[-1]}", progress=(index + 1) / len(COMMANDS))
    return Result(host=task.host, result=", ".join(facts))


class PrintProgress:
    """Processor that prints each partial result, with the thread that reported it."""

    def __init__(self, started_at: float) -> None:
        self.started_at = started_at

    def task_instance_progress(self, task: Task, host: Host, partial: PartialResult) -> None:
        print(f"{format_event(partial, self.started_at)}  [{threading.current_thread().name}]")

    def task_started(self, task: Task) -> None:
        pass

    def task_completed(self, task: Task, result: AggregatedResult[Any]) -> None:
        pass

    def task_instance_started(self, task: Task, host: Host) -> None:
        pass

    def task_instance_completed(self, task: Task, host: Host, result: MultiResult[Any]) -> None:
        print(f"{format_event(result[0], self.started_at)}  [{threading.current_thread().name}]")

    def subtask_instance_started(self, task: Task, host: Host) -> None:
        pass

    def subtask_instance_completed(self, task: Task, host: Host, result: MultiResult[Any]) -> None:
        pass


@app.default
def main(
    *,
    hosts: EosHosts = None,
    runner: SyncRunner = "threaded",
    delay: float = 1.0,
    slow_host: SlowHosts = None,
    slow_delay: float = 3.0,
    fail_host: FailHosts = None,
    break_after: int | None = None,
    processor: bool = False,
    verbose: Verbose = False,
) -> int:
    """Run a fact-collection task that reports progress, and print each event as it arrives.

    Example: stream_progress_sync.py --slow-host ceos3 --fail-host ceos2 --break-after 4

    Parameters
    ----------
    delay
        Seconds each host waits on the device before each command.
    slow_delay
        Seconds a --slow-host waits on the device before each command.
    break_after
        Leave the loop after this many events, which cancels the run.
    processor
        Run with nr.run() and print the partial results from a processor, instead of
        iterating over nr.stream().
    """
    task_args: dict[str, Any] = {
        "delay": delay,
        "slow_hosts": frozenset(slow_host or ()),
        "slow_delay": slow_delay,
        "fail_hosts": frozenset(fail_host or ()),
    }
    with init_nornir_sync(hosts=hosts, platform=PLATFORM, runner=runner) as nr:
        started_at = time.monotonic()
        if processor:
            result = nr.with_processors([PrintProgress(started_at)]).run(task=collect_facts, **task_args)
            print("\nnr.run() returned. Types stored for each host:")
            for host, multi in result.items():
                print(f"  {host}: {[type(item).__name__ for item in multi]}")
        else:
            stream = nr.stream(task=collect_facts, **task_args)
            print(f"nr.stream() returned a {type(stream).__name__}; the {runner} runner runs the hosts")
            print(f"{'elapsed':>7}  {'host':<6} event\n")
            for count, event in enumerate(stream, start=1):
                print(format_event(event, started_at))
                if break_after is not None and count >= break_after:
                    print(f"\nLeaving the loop after {count} events: the run is cancelled.")
                    break
            result = stream.result()
            print(f"\nstream.result() returned after {time.monotonic() - started_at:.2f}s")
    print()
    return print_summary([result], verbose)


if __name__ == "__main__":
    app()
