from abc import ABC, abstractmethod
import base64
from datetime import datetime

import data_types
import database
import constants
import replicas


class Command(ABC):
    @abstractmethod
    def execute(
        self,
        db: database.Database | None,
        replica_handler: replicas.ReplicaHandler | None,
    ) -> str | list[str]: ...


class PingCommand(Command):
    def execute(self, db, replica_handler) -> str:
        return data_types.RespSimpleString("PONG").encode()


class EchoCommand(Command):
    def __init__(self, bulk_str: data_types.RespBulkString):
        self.msg = bulk_str.data

    def execute(self, db, replica_handler) -> str:
        return data_types.RespSimpleString(self.msg).encode()


class SetCommand(Command):
    def __init__(
        self,
        key: data_types.RespBulkString,
        value: data_types.RespBulkString,
        expiry: datetime | None,
    ):
        self.key = key.data
        self.value = value.data
        self.expiry = expiry

    def execute(self, db: database.Database, replica_handler) -> str:
        db[self.key] = (self.value, self.expiry)
        return constants.OK_RESPONSE


class GetCommand(Command):
    def __init__(self, key: data_types.RespBulkString):
        self.key = key.data

    def execute(self, db: database.Database, replica_handler) -> str:
        if self.key in db:
            return data_types.RespBulkString(db[self.key]).encode()
        return constants.NULL_BULK_STRING


class CommandCommand(Command):
    # TODO
    def execute(self, db, replica_handler) -> str:
        return constants.OK_RESPONSE


class InfoCommand(Command):
    def execute(self, db, replica_handler: replicas.ReplicaHandler) -> str:
        return replica_handler.get_info()


class ReplConfCommand(Command):
    def execute(self, db, replica_handler) -> str:
        return constants.OK_RESPONSE


class PsyncCommand(Command):
    def execute(self, db, replica_handler: replicas.ReplicaHandler) -> list[str]:
        return [
            data_types.RespSimpleString(
                f"FULLRESYNC {replica_handler.ip} {replica_handler.info['master_repl_offset']}"
            ).encode(),
            data_types.RdbFile(
                r"REDIS0011\xfa\tredis-ver\x057.2.0\xfa\nredis-bits\xc0@\xfa\x05ctime\xc2m\x08\xbce\xfa\x08used-mem\xc2\xb0\xc4\x10\x00\xfa\x08aof-base\xc0\x00\xff\xf0n;\xfe\xc0\xffZ\xa2"
            ).encode(),
        ]
