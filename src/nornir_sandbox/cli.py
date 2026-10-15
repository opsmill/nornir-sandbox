"""Command-line options and output shared by the scripts in scripts/.

Scripts are cyclopts apps. Annotate a parameter with one of the types below to
get the same option, with the same help text, in every script.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Annotated, Any

from cyclopts import Parameter
from nornir.core.task import AggregatedResult

from nornir_sandbox.core import init_nornir


def _known_hosts(type_: Any, value: list[str] | None) -> None:
    """Reject host names that are not in the inventory, as a usage error instead of a traceback."""
    if value:
        init_nornir(hosts=value)


Hosts = Annotated[
    list[str] | None,
    Parameter(
        help="Host to work on; repeat the option for several hosts. Default: every host.",
        negative=(),
        validator=_known_hosts,
    ),
]
Verbose = Annotated[bool, Parameter(help="Print the traceback of failed tasks.", negative=())]


def print_summary(results: Iterable[AggregatedResult], verbose: bool = False) -> int:
    """Print one line per host and task. Return 1 if any task failed, else 0."""
    failed = False
    for aggregated in results:
        for host, multi in sorted(aggregated.items()):
            for result in multi:
                failed |= result.failed
                status = "FAILED" if result.failed else "OK"
                if result.exception is not None:
                    message = f"{type(result.exception).__name__}: {result.exception}"
                else:
                    message = str(result.result)
                print(f"{host:<10} {result.name:<22} {status:<7} {message}")
                if verbose and result.exception is not None:
                    print(result.result)
    if failed:
        print("\nSome tasks failed. Run again with --verbose, or read nornir.log, for details.")
    return int(failed)
