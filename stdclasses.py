from dataclasses import dataclass
from typing import Any


@dataclass
class RawContainer:
    type: bytes
    length: bytes
    data_index: bytes
    data_id_length: bytes
    data_id: bytes
    payload: bytes


@dataclass
class SchemaField:
    name: str
    type: str
    pos: int
    length: int
    tags: dict
    payload: bytes
    data: Any
