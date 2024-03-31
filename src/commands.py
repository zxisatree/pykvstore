from abc import ABC, abstractmethod
from datetime import datetime
import socket
import threading
from datetime import timedelta
from queue import Queue

import constants
import data_types
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
        db: database.Database | None,
        replica_handler: replicas.ReplicaHandler | None,
        conn: socket.socket | None,
    ) -> bytes | list[bytes]: ...


class PingCommand(Command):
    def __init__(self, raw_cmd: bytes):
        self._raw_cmd = raw_cmd

    def execute(self, db, replica_handler, conn) -> bytes:
        return data_types.RespSimpleString(b"PONG").encode()


class EchoCommand(Command):
    def __init__(self, raw_cmd: bytes, bulk_str: data_types.RespBulkString):
        self._raw_cmd = raw_cmd
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
        self._raw_cmd = raw_cmd
        self.key = key.data
        self.value = value.data
        self.expiry = expiry

    def execute(
        self, db: database.Database, replica_handler: replicas.ReplicaHandler, conn
    ) -> bytes:
        replica_handler.propogate(self._raw_cmd)
        db[self.key.decode()] = (self.value.decode(), self.expiry)
        return constants.OK_SIMPLE_STRING.encode()


class GetCommand(Command):
    def __init__(self, raw_cmd: bytes, key: data_types.RespBulkString):
        self._raw_cmd = raw_cmd
        self.key = key.data

    def execute(self, db: database.Database, replica_handler, conn) -> bytes:
        if self.key.decode() in db:
            return data_types.RespBulkString(db[self.key.decode()].encode()).encode()
        return constants.NULL_BULK_STRING.encode()


class CommandCommand(Command):
    # TODO
    def __init__(self, raw_cmd: bytes):
        self._raw_cmd = raw_cmd

    def execute(self, db, replica_handler, conn) -> bytes:
        return constants.OK_SIMPLE_STRING.encode()


class InfoCommand(Command):
    def __init__(self, raw_cmd: bytes):
        self._raw_cmd = raw_cmd

    def execute(self, db, replica_handler: replicas.ReplicaHandler, conn) -> bytes:
        return replica_handler.get_info()


class ReplConfCommand(Command):
    def __init__(self, raw_cmd: bytes):
        self._raw_cmd = raw_cmd

    def execute(self, db, replica_handler, conn) -> bytes:
        return constants.OK_SIMPLE_STRING.encode()


class ReplConfAckCommand(Command):
    def __init__(self, raw_cmd: bytes):
        self._raw_cmd = raw_cmd

    def execute(self, db, replica_handler: replicas.ReplicaHandler, conn) -> bytes:
        print(
            f"incrementing {replica_handler.ack_count=} to {replica_handler.ack_count + 1}"
        )
        replica_handler.ack_count += 1
        return constants.OK_SIMPLE_STRING.encode()


class ReplConfGetAckCommand(Command):
    def __init__(self, raw_cmd: bytes):
        self._raw_cmd = raw_cmd

    def execute(self, db, replica_handler: replicas.ReplicaHandler, conn) -> bytes:
        replica_handler.propogate(self._raw_cmd)
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
    def __init__(self, raw_cmd: bytes):
        self._raw_cmd = raw_cmd

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
        self._raw_cmd = data

    def execute(self, db, replica_handler, conn) -> bytes:
        return b""


class RdbFileCommand(Command):
    def __init__(self, data: bytes) -> None:
        self.data = data
        self._raw_cmd = data

    # slave received a RDB file
    def execute(self, db, replica_handler: replicas.ReplicaHandler, conn) -> bytes:
        return b""


class WaitCommand(Command):
    def __init__(self, raw_cmd: bytes, replica_count: int, timeout: int):
        self._raw_cmd = raw_cmd
        self.replica_count = replica_count
        self.timeout = timedelta(milliseconds=timeout)

    def execute(
        self, db, replica_handler: replicas.ReplicaHandler, conn: socket.socket
    ) -> bytes:
        print(f"executing WaitCommand, {replica_handler.is_master=}")
        replica_handler.ack_count = 0
        for slave in replica_handler.slaves:
            print(f"sending to {slave=}")
            slave.sendall(
                data_types.RespArray(
                    [
                        data_types.RespBulkString(b"REPLCONF"),
                        data_types.RespBulkString(b"GETACK"),
                        data_types.RespBulkString(b"*"),
                    ]
                ).encode()
            )
        end = datetime.now() + self.timeout
        print(f"finished sending to all slaves, {end-datetime.now()=}")
        while replica_handler.ack_count < self.replica_count and datetime.now() < end:
            # recv from slaves?
            print(f"recving from {slave=}")
            data = slave.recv(constants.BUFFER_SIZE)
            replica_handler.ack_count += 1
            print(f"after sending getack to slave, received {data=}")

        print(
            f"{replica_handler.ack_count=}, {datetime.now() - end=} (should be positive)"
        )
        return data_types.RespInteger(replica_handler.ack_count).encode()
