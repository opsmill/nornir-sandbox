"""Command-line options and output shared by the scripts in scripts/.

Scripts are cyclopts apps. Annotate a parameter with one of the types below to
get the same option, with the same help text, in every script.
"""

from __future__ import annotations

import time
from collections.abc import Iterable
from typing import Annotated, Any, Literal

from cyclopts import Parameter
from nornir.core.exceptions import TaskInterruptedError
from nornir.core.task import AggregatedResult, PartialResult, Result

from nornir_sandbox.core import init_nornir_sync


def _known_hosts(type_: Any, value: list[str] | None) -> None:
    """Reject host names that are not in the inventory, as a usage error instead of a traceback.

    cyclopts validates the options before it starts the event loop, so this uses the
    synchronous `init_nornir_sync()`.
    """
    if value:
        init_nornir_sync(hosts=value)


Hosts = Annotated[
    list[str] | None,
    Parameter(
        help="Host to work on; repeat the option for several hosts. Default: every host.",
        negative=(),
        validator=_known_hosts,
    ),
]
Verbose = Annotated[bool, Parameter(help="Print the traceback of failed tasks.", negative=())]

# Options of the scripts in scripts/runner/, which inject delays and failures into the
# collect_facts task defined in each script.
SlowHosts = Annotated[
    list[str] | None,
    Parameter(
        help="Host that waits --slow-delay seconds on the device before each command, instead of --delay; "
        "repeat the option for several hosts.",
        negative=(),
        validator=_known_hosts,
    ),
]
FailHosts = Annotated[
    list[str] | None,
    Parameter(
        help="Host whose second command is rejected by EOS, so its task fails; repeat the option for several hosts.",
        negative=(),
        validator=_known_hosts,
    ),
]
SyncRunner = Annotated[
    Literal["threaded", "serial"],
    Parameter(help="Nornir runner: threaded runs the hosts in threads, serial one after the other on this thread."),
]


def describe_result(result: Result) -> str:
    """Return the exception type and message of a failed result, or the result value.

    For a cancelled or timed-out host, also say whether its task had started.
    """
    if result.exception is None:
        return str(result.result)
    message = f"{type(result.exception).__name__}: {result.exception}"
    if isinstance(result.exception, TaskInterruptedError):
        message += f" (started={result.exception.started})"
    return message


def format_event(event: PartialResult | Result, started_at: float) -> str:
    """Return one line for an event of a run: seconds since `started_at` (time.monotonic()), host, event."""
    elapsed = time.monotonic() - started_at
    host = event.host.name if event.host is not None else "-"
    if isinstance(event, PartialResult):
        progress = "    " if event.progress is None else f"{event.progress:4.0%}"
        return f"{elapsed:6.2f}s  {host:<6} progress {progress}  {event.result}"
    status = "FAILED  " if event.failed else "finished"
    return f"{elapsed:6.2f}s  {host:<6} {status}       {describe_result(event)}"


def print_summary(results: Iterable[AggregatedResult], verbose: bool = False) -> int:
    """Print one line per host and task. Return 1 if any task failed, else 0."""
    failed = False
    for aggregated in results:
        for host, multi in sorted(aggregated.items()):
            for result in multi:
                failed |= result.failed
                status = "FAILED" if result.failed else "OK"
                print(f"{host:<10} {result.name:<22} {status:<7} {describe_result(result)}")
                if verbose and result.exception is not None:
                    print(result.result)
    if failed:
        print("\nSome tasks failed. Run again with --verbose, or read nornir.log, for details.")
    return int(failed)
