from abc import ABC, abstractmethod

import codec


class RespDataType(ABC):
    @abstractmethod
    def encode(self) -> str: ...

    @staticmethod
    @abstractmethod
    # Returns the parsed object and the new pos
    def decode(data: str, pos: int) -> tuple["RespDataType", int]: ...


class RespSimpleString(RespDataType):
    def __init__(self, data: str):
        self.data = data

    def __str__(self) -> str:
        return str(self.data)

    def __repr__(self) -> str:
        return f"RespSimpleString({repr(self.data)})"

    def encode(self) -> str:
        return f"+{self.data}\r\n"

    @staticmethod
    def decode(data: str, pos: int) -> tuple["RespSimpleString", int]:
        start = pos
        while pos < len(data) and not codec.is_sep(data, pos):
            pos += 1
        if pos >= len(data):
            print("Invalid RESP simple string, missing \\r\\n separator")
        simple_str = data[start:pos]
        pos += 2
        assert pos <= len(data)
        return (RespSimpleString(simple_str), pos)


class RespArray(RespDataType):
    def __init__(self, elements: list[RespDataType]):
        self.elements = elements

    def __getitem__(self, idx) -> list[RespDataType] | RespDataType:
        res = self.elements.__getitem__(idx)
        if isinstance(res, list):
            return list(res)  # enables type hinting
        else:
            return res

    def __setitem__(self, idx, value: RespDataType):
        self.elements.__setitem__(idx, value)

    def __delitem__(self, idx):
        self.elements.__delitem__(idx)

    def __str__(self) -> str:
        return str(self.elements)

    def __repr__(self) -> str:
        return f"RespArray({repr(self.elements)})"

    def encode(self) -> str:
        return f"*{len(self.elements)}\r\n" + "\r\n".join(
            map(lambda x: x.encode(), self.elements)
        )

    @staticmethod
    def decode(data: str, pos: int) -> tuple["RespArray", int]:
        start = pos
        while pos < len(data) and not codec.is_sep(data, pos):
            print(f"{pos=}, {data[pos]=}")
            pos += 1
        if pos >= len(data):
            print("Invalid RESP array, missing \\r\\n separator")
        array_len = int(data[start:pos])
        pos += 2

        elements: list[RespDataType] = []
        for _ in range(array_len):
            element, pos = codec.parse(data, pos)
            elements.append(element)
        assert pos <= len(data)
        return (RespArray(elements), pos)


class RespBulkString(RespDataType):
    def __init__(self, data: str):
        self.data = data

    def __str__(self) -> str:
        return str(self.data)

    def __repr__(self) -> str:
        return f"RespBulkString({repr(self.data)})"

    def encode(self) -> str:
        return f"${len(self.data)}\r\n{self.data}\r\n" if len(self.data) else "$-1\r\n"

    @staticmethod
    def decode(data: str, pos: int) -> tuple["RespBulkString", int]:
        start = pos
        while pos < len(data) and not codec.is_sep(data, pos):
            pos += 1
        if pos >= len(data):
            print("Invalid RESP bulk string, missing \\r\\n separator")
        bulk_str_len = int(data[start:pos])
        pos += 2

        bulk_str = data[pos : pos + bulk_str_len]
        pos += bulk_str_len + 2
        assert pos <= len(data)
        return (RespBulkString(bulk_str), pos)
