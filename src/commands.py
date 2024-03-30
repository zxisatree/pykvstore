from abc import ABC, abstractmethod
from datetime import datetime
import socket

import constants
import data_types
import database
import replicas


class Command(ABC):
    @abstractmethod
    def execute(
        self,
        db: database.Database | None,
        replica_handler: replicas.ReplicaHandler | None,
        conn: socket.socket | None,
    ) -> bytes | list[bytes]: ...


class PingCommand(Command):
    def execute(self, db, replica_handler, conn) -> bytes:
        return data_types.RespSimpleString(b"PONG").encode()


class EchoCommand(Command):
    def __init__(self, bulk_str: data_types.RespBulkString):
        self.msg = bulk_str.data

    def execute(self, db, replica_handler, conn) -> bytes:
        return data_types.RespSimpleString(self.msg).encode()


class SetCommand(Command):
    def __init__(
        self,
        raw_cmd: bytes,
        key: data_types.RespBulkString,
        value: data_types.RespBulkString,
        expiry: datetime | None,
    ):
        self.raw_cmd = raw_cmd
        self.key = key.data
        self.value = value.data
        self.expiry = expiry

    def execute(
        self, db: database.Database, replica_handler: replicas.ReplicaHandler, conn
    ) -> bytes:
        replica_handler.propogate(self.raw_cmd)
        db[self.key.decode()] = (self.value.decode(), self.expiry)
        return constants.OK_SIMPLE_STRING.encode()


class GetCommand(Command):
    def __init__(self, key: data_types.RespBulkString):
        self.key = key.data

    def execute(self, db: database.Database, replica_handler, conn) -> bytes:
        if self.key.decode() in db:
            return data_types.RespBulkString(db[self.key.decode()].encode()).encode()
        return constants.NULL_BULK_STRING.encode()


class CommandCommand(Command):
    # TODO
    def execute(self, db, replica_handler, conn) -> bytes:
        return constants.OK_SIMPLE_STRING.encode()


class InfoCommand(Command):
    def execute(self, db, replica_handler: replicas.ReplicaHandler, conn) -> bytes:
        return replica_handler.get_info()


class ReplConfCommand(Command):
    def execute(self, db, replica_handler, conn) -> bytes:
        return constants.OK_SIMPLE_STRING.encode()


class ReplConfGetAckCommand(Command):
    def __init__(self, raw_cmd: bytes):
        self.raw_cmd = raw_cmd

    def execute(self, db, replica_handler: replicas.ReplicaHandler, conn) -> bytes:
        replica_handler.propogate(self.raw_cmd)
        return data_types.RespArray(
            [
                data_types.RespBulkString(b"REPLCONF"),
                data_types.RespBulkString(b"ACK"),
                data_types.RespBulkString(
                    str(replica_handler.info["master_repl_offset"]).encode()
                ),
            ]
        ).encode()


class PsyncCommand(Command):
    def execute(
        self, db, replica_handler: replicas.ReplicaHandler, conn: socket.socket
    ) -> list[bytes]:
        replica_handler.slaves.append(conn)
        return [
            data_types.RespSimpleString(
                f"FULLRESYNC {replica_handler.ip} {replica_handler.info['master_repl_offset']}".encode()
            ).encode(),
            data_types.RdbFile(b"").encode(),
        ]


class FullResyncCommand(Command):
    def __init__(self, data: bytes) -> None:
        self.data = data

    def execute(self, db, replica_handler, conn) -> bytes:
        return b""


class RdbFileCommand(Command):
    def __init__(self, data: bytes) -> None:
        self.data = data

    # slave received a RDB file
    def execute(self, db, replica_handler: replicas.ReplicaHandler, conn) -> bytes:
        return b""
