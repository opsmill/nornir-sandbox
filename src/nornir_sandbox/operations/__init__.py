"""Operations: actions defined once with a typed input and output, implemented per platform.

- contracts.py: the Operations and their input and output models. No device logic.
- parse.py: pure functions that turn device output into the output models; unit tested.
- eos.py, srlinux.py: the tasks that implement each Operation for one platform, in an
  `async def` form for the asyncio runner and a `def` form for the threaded and serial
  runners.

pyproject.toml registers the contracts and the tasks as entry points, so Nornir finds
them the first time a script runs an Operation. A registration key is
`<operation>.<platform>.<connection>` and holds one task, either sync or async. Both
forms use the scrapli connection, so the async tasks are registered under the
connection name `scrapli` and the sync tasks under `scrapli_sync`; init_nornir() and init_nornir_sync() set
`core.operation_connection` to the one that matches the runner.
"""

from nornir_sandbox.operations.contracts import (
    ASYNC_CONNECTION,
    SYNC_CONNECTION,
    Config,
    Facts,
    GetConfig,
    GetConfigInput,
    GetFacts,
    GetFactsInput,
    GetInterfaces,
    GetInterfacesInput,
    GetLldpNeighbors,
    GetLldpNeighborsInput,
    Interface,
    Interfaces,
    LldpNeighbors,
    SetInterfaceDescriptions,
    SetInterfaceDescriptionsInput,
    SetInterfaceDescriptionsOutput,
)

__all__ = [
    "ASYNC_CONNECTION",
    "SYNC_CONNECTION",
    "Config",
    "Facts",
    "GetConfig",
    "GetConfigInput",
    "GetFacts",
    "GetFactsInput",
    "GetInterfaces",
    "GetInterfacesInput",
    "GetLldpNeighbors",
    "GetLldpNeighborsInput",
    "Interface",
    "Interfaces",
    "LldpNeighbors",
    "SetInterfaceDescriptions",
    "SetInterfaceDescriptionsInput",
    "SetInterfaceDescriptionsOutput",
]
