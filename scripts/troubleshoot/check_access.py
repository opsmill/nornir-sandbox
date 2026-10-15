from __future__ import annotations

import time

from cyclopts import App

from nornir_sandbox import init_nornir
from nornir_sandbox.cli import Hosts, Verbose, print_summary
from nornir_sandbox.tasks import check_access

app = App()


@app.default
def main(*, hosts: Hosts = None, wait: int = 0, interval: int = 10, verbose: Verbose = False) -> int:
    """Check that each lab device answers on SSH, accepts the login and runs commands.

    cEOS accepts SSH logins about 30 seconds after `containerlab deploy`.

    Parameters
    ----------
    wait
        Seconds to keep retrying the hosts that fail.
    interval
        Seconds between retries.
    """
    nr = init_nornir(hosts=hosts)
    deadline = time.monotonic() + wait
    pending = set(nr.inventory.hosts)
    passed = {}

    while True:
        targets = nr.filter(filter_func=lambda host, names=frozenset(pending): host.name in names)
        result = targets.run(task=check_access, on_failed=True)
        passed.update({host: multi for host, multi in result.items() if not multi.failed})
        pending = {host for host, multi in result.items() if multi.failed}
        if not pending or time.monotonic() >= deadline:
            break
        print(f"waiting for {', '.join(sorted(pending))} ({int(deadline - time.monotonic())}s left)", flush=True)
        time.sleep(interval)

    # Hosts that passed in an earlier round are reported with their passing result.
    result.update(passed)
    return print_summary([result], verbose=verbose)


if __name__ == "__main__":
    app()
