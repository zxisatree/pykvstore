from __future__ import annotations
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import socket

    import database
    import replicas


class Command(ABC):
    def __init__(self):
        self._raw_cmd = b""

    @property
    def raw_cmd(self) -> bytes:
        return self._raw_cmd

    @abstractmethod
    def execute(
        self,
        db: database.Database,
        replica_handler: replicas.ReplicaHandler,
        conn: socket.socket,
    ) -> bytes | list[bytes]: ...

    @staticmethod
    @abstractmethod
    # might raise RequestCraftError
    def craft_request(*args: str) -> "Command": ...
