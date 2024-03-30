from abc import ABC, abstractmethod
from datetime import datetime

import data_types
import database
import constants


class Command(ABC):
    @abstractmethod
    def execute(self, db: database.Database) -> str: ...


class PingCommand(Command):
    def execute(self, db: database.Database) -> str:
        return data_types.RespSimpleString("PONG").encode()


class EchoCommand(Command):
    def __init__(self, bulk_str: data_types.RespBulkString):
        self.msg = bulk_str.data

    def execute(self, db: database.Database) -> str:
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

    def execute(self, db: database.Database) -> str:
        db[self.key] = (self.value, self.expiry)
        return constants.OK_RESPONSE


class GetCommand(Command):
    def __init__(self, key: data_types.RespBulkString):
        self.key = key.data

    def execute(self, db: database.Database) -> str:
        if self.key in db:
            return data_types.RespBulkString(db[self.key]).encode()
        return constants.NULL_BULK_STRING


class CommandCommand(Command):
    # TODO
    def execute(self, db: database.Database) -> str:
        return constants.OK_RESPONSE


class InfoCommand(Command):
    # TODO
    def execute(self, db: database.Database) -> str:
        return constants.OK_RESPONSE
