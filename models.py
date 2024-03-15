from dataclasses import dataclass
from enum import Enum
from typing import Optional, TypedDict
from dane.provenance import Provenance


class CallbackResponse(TypedDict):
    """Response returned by callback(), with state and message"""

    state: int
    message: str


class OutputType(Enum):
    """Types of output this worker (possibly) provides (depending on config)"""

    FOOBAR = "foobar"
    PROVENANCE = "provenance"  # produced by provenance.py


@dataclass
class ThisWorkerInput:
    """Dataclass that specifies any input this worker depends on.

    state (+message) denotes whether everything is good to go"""

    state: int  # HTTP status code
    message: str  # error/success message
    source_id: str = ""  # <program ID>__<carrier ID>
    input_file_path: str = ""  # where the input was downloaded from
    provenance: Optional[Provenance] = None  # mostly: how long did it take to download


@dataclass
class ThisWorkerOutput:
    """Dataclass that specifies any output this worker will produce"""

    state: int  # HTTP status code
    message: str  # error/success message
    output_file_path: str = ""  # where to store the worker's output
    provenance: Optional[Provenance] = None  # this worker's provenance
