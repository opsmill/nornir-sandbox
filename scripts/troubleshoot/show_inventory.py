from __future__ import annotations

from cyclopts import App

from nornir_sandbox import init_nornir
from nornir_sandbox.cli import Hosts

app = App()


@app.default
def main(*, hosts: Hosts = None) -> None:
    """Print the Nornir inventory from inventory/, without connecting to any device."""
    for name, host in sorted(init_nornir(hosts=hosts).inventory.hosts.items()):
        print(name)
        print(f"  hostname: {host.hostname}")
        print(f"  platform: {host.platform}")
        print(f"  groups:   {', '.join(g.name for g in host.groups)}")
        print("  links:")
        for link in host.get("links", []):
            print(f"    {link['interface']:<12} -> {link['peer']} {link['peer_interface']}")


if __name__ == "__main__":
    app()
