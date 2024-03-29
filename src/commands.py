from abc import ABC, abstractmethod

import data_types
import constants


class Command(ABC):
    @abstractmethod
    def execute(self, cache: dict) -> str: ...


class PingCommand(Command):
    def execute(self, cache: dict) -> str:
        return data_types.RespSimpleString("PONG").encode()


class EchoCommand(Command):
    def __init__(self, bulk_str: data_types.RespBulkString):
        self.msg = bulk_str.data

    def execute(self, cache: dict) -> str:
        return data_types.RespSimpleString(self.msg).encode()


class SetCommand(Command):
    def __init__(
        self, key: data_types.RespBulkString, value: data_types.RespBulkString
    ):
        self.key = key.data
        self.value = value.data

    def execute(self, cache: dict) -> str:
        cache[self.key] = self.value
        return constants.OK_RESPONSE


class GetCommand(Command):
    def __init__(self, key: data_types.RespBulkString):
        self.key = key.data

    def execute(self, cache: dict) -> str:
        if self.key in cache:
            return data_types.RespBulkString(cache[self.key]).encode()
        return constants.NULL_BULK_STRING


class CommandCommand(Command):
    # TODO
    def execute(self, cache: dict) -> str:
        return constants.OK_RESPONSE


class InfoCommand(Command):
    # TODO
    def execute(self, cache: dict) -> str:
        return constants.OK_RESPONSE
