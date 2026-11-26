"""Start a run in the background with nr.submit(), and cancel it when a host fails.

nr.submit() returns a RunHandle at once; the threaded runner runs the hosts in a
background thread. handle.results() yields the final Result of each host in the order
the hosts finish. On the first failed host, the script calls handle.cancel(): hosts
that are running stop at their next checkpoint (a `yield` of the generator task, or
task.checkpoint()), and hosts that have not started never start.

--watch starts a second thread that prints handle.progress(), the latest partial
result of each host, every half second: any thread can read the handle.

The serial runner runs every task on the caller's thread, so it cannot run in the
background: with --runner serial, nr.submit() raises RunnerCapabilityError.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Collection, Generator
from typing import Any

from cyclopts import App
from nornir.core.exceptions import RunnerCapabilityError
from nornir.core.run import RunHandle
from nornir.core.task import PartialResult, Result, Task

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

    Each `yield` is a checkpoint: after handle.cancel(), a running host stops at its
    next `yield`. The wait on the device blocks the thread until it ends. A host in
    `fail_hosts` sends REJECTED_COMMAND before its second command, which raises
    EOSCommandError and fails the task.
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


def watch_progress(handle: RunHandle[Any], started_at: float) -> None:
    """Print the latest partial result of each host every half second, until the run is done."""
    while not handle.done:
        snapshot = ", ".join(
            f"{host}={partial.progress:.0%}" if partial.progress is not None else f"{host}={partial.result}"
            for host, partial in sorted(handle.progress().items())
        )
        print(f"{time.monotonic() - started_at:6.2f}s  [watch] {snapshot or 'no progress yet'}")
        time.sleep(0.5)


@app.default
def main(
    *,
    hosts: EosHosts = None,
    runner: SyncRunner = "threaded",
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

    Example: cancel_on_failure_sync.py --fail-host ceos2 --slow-host ceos3 --watch

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
        Print handle.progress() every half second from another thread.
    """
    with init_nornir_sync(hosts=hosts, platform=PLATFORM, runner=runner, num_workers=workers) as nr:
        started_at = time.monotonic()
        try:
            handle = nr.submit(
                task=collect_facts,
                delay=delay,
                slow_hosts=frozenset(slow_host or ()),
                slow_delay=slow_delay,
                fail_hosts=frozenset(fail_host or ()),
            )
        except RunnerCapabilityError as e:
            print(f"nr.submit() raised {type(e).__name__}: {e}")
            return 2
        print(f"submit() returned run {handle.id} after {time.monotonic() - started_at:.3f}s; done={handle.done}\n")
        watcher = threading.Thread(target=watch_progress, args=(handle, started_at), daemon=True)
        if watch:
            watcher.start()

        for result in handle.results():
            print(format_event(result, started_at))
            if result.failed and not keep_going:
                print(f"{time.monotonic() - started_at:6.2f}s  calling handle.cancel()")
                handle.cancel()
                break

        aggregated = handle.result()
        print(f"\nhandle.result() returned after {time.monotonic() - started_at:.2f}s; done={handle.done}")
        if watch:
            watcher.join()
        print(f"nr.data.failed_hosts: {sorted(nr.data.failed_hosts)}\n")
    return print_summary([aggregated], verbose)


if __name__ == "__main__":
    app()
