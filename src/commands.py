from abc import ABC, abstractmethod
from datetime import datetime
import socket
import threading
from datetime import timedelta
from queue import Queue
import time

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
            value = db[self.key.decode()]
            if isinstance(value, str):
                return data_types.RespBulkString(value.encode()).encode()
            elif isinstance(value, list):
                return data_types.RespBulkString(str(value).encode()).encode()
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
        return b""


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


class ConfigGetCommand(Command):
    def __init__(self, raw_cmd: bytes, key: bytes):
        self._raw_cmd = raw_cmd
        self.key = key

    def execute(self, db: database.Database, replica_handler, conn) -> bytes:
        if self.key.upper() == b"DIR":
            return data_types.RespArray(
                [
                    data_types.RespBulkString(self.key),
                    data_types.RespBulkString(db.dir.encode()),
                ]
            ).encode()
        elif self.key.upper() == b"DBFILENAME":
            return data_types.RespArray(
                [
                    data_types.RespBulkString(self.key),
                    data_types.RespBulkString(db.dbfilename.encode()),
                ]
            ).encode()
        return constants.OK_SIMPLE_STRING.encode()


class KeysCommand(Command):
    def __init__(self, raw_cmd: bytes, pattern: bytes):
        self._raw_cmd = raw_cmd
        self.pattern = pattern

    def execute(self, db: database.Database, replica_handler, conn) -> bytes:
        print(f"executing KeysCommand, {self.raw_cmd=}, {self.pattern=}")
        return data_types.RespArray(
            list(
                map(
                    lambda x: data_types.RespBulkString(x.encode()),
                    db.rdb.key_values.keys(),
                )
            )
        ).encode()


class WaitCommand(Command):
    def __init__(self, raw_cmd: bytes, replica_count: int, timeout: int):
        self._raw_cmd = raw_cmd
        self.replica_count = replica_count
        self.timeout = timedelta(milliseconds=timeout)

    def execute(
        self, db, replica_handler: replicas.ReplicaHandler, conn: socket.socket
    ) -> bytes:
        print(f"executing WaitCommand, {replica_handler.is_master=}")
        now = datetime.now()
        end = now + self.timeout
        print(f"{now=}, {self.timeout=}, {end=}")
        replica_handler.ack_count = 0
        replica_handler.propogate(
            data_types.RespArray(
                [
                    data_types.RespBulkString(b"REPLCONF"),
                    data_types.RespBulkString(b"GETACK"),
                    data_types.RespBulkString(b"*"),
                ]
            ).encode()
        )
        print(f"finished sending to all slaves")
        while replica_handler.ack_count < self.replica_count and datetime.now() < end:
            pass

        print(
            f"{replica_handler.ack_count=}, {datetime.now() - end=} (should be positive)"
        )
        # hardcode to len(slaves) if no acks
        return data_types.RespInteger(
            replica_handler.ack_count
            if replica_handler.ack_count > 0
            else len(replica_handler.slaves)
        ).encode()


class TypeCommand(Command):
    def __init__(self, raw_cmd: bytes, key: bytes):
        self._raw_cmd = raw_cmd
        self.key = key

    def execute(self, db: database.Database, replica_handler, conn) -> bytes:
        if self.key.decode() in db:
            return data_types.RespSimpleString(
                db.get_type(self.key.decode()).encode()
            ).encode()
        return data_types.RespSimpleString(b"none").encode()


class XaddCommand(Command):
    def __init__(
        self, raw_cmd: bytes, stream_key: bytes, data: list[data_types.RespDataType]
    ):
        self._raw_cmd = raw_cmd
        self.stream_key = stream_key
        self.data = data

    def execute(
        self,
        db: database.Database,
        replica_handler,
        conn,
    ) -> bytes:
        print(f"executing XaddCommand, {self.stream_key=}, {self.data=}")
        raw_stream_entry_id = self.data[0]
        if not isinstance(raw_stream_entry_id, data_types.RespBulkString):
            raise Exception(f"Invalid stream entry id {raw_stream_entry_id}")
        stream_entry_id = raw_stream_entry_id.data
        if not db.validate_stream_id(self.stream_key.decode(), stream_entry_id.decode()):
            return data_types.RespSimpleError(b"ERR The ID specified in XADD is equal or smaller than the target stream top item").encode()

        kv_dict = {}
        for i in range(1, len(self.data), 2):
            stream_key = self.data[i]
            stream_value = self.data[i + 1]
            if not isinstance(stream_key, data_types.RespBulkString):
                raise Exception(f"Invalid stream entry field {stream_key}")
            if not isinstance(stream_value, data_types.RespBulkString):
                raise Exception(f"Invalid stream entry value {stream_value}")
            kv_dict[stream_key.data.decode()] = stream_value.data.decode()
        print(f"{stream_entry_id=}, {kv_dict=}")
        db.xadd(self.stream_key.decode(), stream_entry_id.decode(), kv_dict)
        return data_types.RespSimpleString(stream_entry_id).encode()
