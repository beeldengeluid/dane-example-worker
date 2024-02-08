from dataclasses import dataclass
from enum import Enum
from typing import Optional, TypedDict
from dane.provenance import Provenance


# returned by callback()
class CallbackResponse(TypedDict):
    state: int
    message: str


# These are the types of output this worker (possibly) provides (depending on config)
class OutputType(Enum):
    FOOBAR = "foobar"
    PROVENANCE = "provenance"  # produced by provenance.py


@dataclass
class ThisWorkerInput:
    state: int  # HTTP status code
    message: str  # error/sucess message
    source_id: str = ""  # <program ID>__<carrier ID>
    input_file_path: str = ""  # where the input was downloaded from
    provenance: Optional[Provenance] = None  # mostly: how long did it take to download


@dataclass
class ThisWorkerOutput:
    state: int  # HTTP status code
    message: str  # error/success message
    output_file_path: str = ""  # where to store the worker's output
    provenance: Optional[Provenance] = None  # this worker's provenance
