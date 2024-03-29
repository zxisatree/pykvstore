from abc import ABC, abstractmethod

import data_types


class Command(ABC):
    @abstractmethod
    def execute(self) -> str: ...


class PingCommand(Command):
    def execute(self) -> str:
        return "+PONG\r\n"


class EchoCommand(Command):
    def __init__(self, bulk_str: data_types.RespBulkString):
        self.msg = bulk_str.data

    def execute(self) -> str:
        return f"+{self.msg}\r\n"


class CommandCommand(Command):
    # TODO
    def execute(self) -> str:
        return "+OK\r\n"


class InfoCommand(Command):
    # TODO
    def execute(self) -> str:
        return "+OK\r\n"
