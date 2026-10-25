"""List the Operations that Nornir knows, with their models and tasks.

OperationRegistry.operations() reads the entry points of the installed packages, here
the ones declared in pyproject.toml, and calls no task, so this script does not
connect to any device.
"""

from __future__ import annotations

from cyclopts import App
from nornir.core.operations import OperationRegistry

app = App()


@app.default
def main() -> None:
    """Print each Operation, its input and output models, and its task for each platform and connection."""
    for info in OperationRegistry.operations():
        models = (
            f"{info.input_model.__name__} -> {info.output_model.__name__}"
            if info.input_model and info.output_model
            else "no Operation class"
        )
        print(f"{info.name}: {models}")
        for registration in info.implementations:
            key = f"{registration.platform}.{registration.connection}"
            task = f"{registration.task.__module__}.{registration.task.__qualname__}"
            print(f"    {key:<28} {registration.kind:<6} {task}")


if __name__ == "__main__":
    app()
