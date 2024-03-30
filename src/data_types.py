from abc import ABC, abstractmethod
from base64 import b64decode

import codec
import constants


class RespDataType(ABC):
    @abstractmethod
    def __len__(self) -> int: ...

    @abstractmethod
    def encode(self) -> bytes: ...

    @staticmethod
    @abstractmethod
    # Returns the parsed object and the new pos
    def decode(data: bytes, pos: int) -> tuple["RespDataType", int]: ...


class RespSimpleString(RespDataType):
    def __init__(self, data: bytes):
        self.data = data

    def __len__(self) -> int:
        return len(self.data)

    def __str__(self) -> str:
        return str(self.data)

    def __repr__(self) -> str:
        return f"RespSimpleString({repr(self.data)})"

    def encode(self) -> bytes:
        return b"+" + self.data + b"\r\n"

    @staticmethod
    def decode(data: bytes, pos: int) -> tuple["RespSimpleString", int]:
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

    def __len__(self) -> int:
        return len(self.elements)

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

    def encode(self) -> bytes:
        return f"*{len(self.elements)}\r\n".encode() + b"".join(
            map(lambda x: x.encode(), self.elements)
        )

    @staticmethod
    def decode(data: bytes, pos: int) -> tuple["RespArray", int]:
        start = pos + 1
        while pos < len(data) and not codec.is_sep(data, pos):
            pos += 1
        if pos >= len(data):
            print("Invalid RESP array, missing \\r\\n separator")
        array_len = int(data[start:pos])
        pos += 2

        elements: list[RespDataType] = []
        for _ in range(array_len):
            print(f"Array.decode: {pos=}, {data[pos:]=}")
            element, pos = codec.parse(data, pos)
            elements.append(element)
        assert pos <= len(data)
        return (RespArray(elements), pos)


class RespBulkString(RespDataType):
    def __init__(self, data: bytes):
        self.data = data

    def __len__(self) -> int:
        return len(self.data)

    def __str__(self) -> str:
        return str(self.data)

    def __repr__(self) -> str:
        return f"RespBulkString({repr(self.data)})"

    def encode(self) -> bytes:
        return (
            f"${len(self.data)}\r\n".encode() + self.data + b"\r\n"
            if self.data
            else constants.NULL_BULK_STRING.encode()
        )

    @staticmethod
    def decode(data: bytes, pos: int) -> tuple["RespBulkString", int]:
        start = pos + 1
        while pos < len(data) and not codec.is_sep(data, pos):
            pos += 1
        print(
            f"{data=}, {start=}, {pos=}, {data[start:]=}, {data[pos:]=}, {data[start:pos]=}"
        )
        if pos >= len(data):
            print("Invalid RESP bulk string, missing \\r\\n separator")
        bulk_str_len = int(data[start:pos])
        pos += 2

        bulk_str = data[pos : pos + bulk_str_len]
        pos += bulk_str_len + 2
        print(f"{data=}, {pos=}, {len(data)=}")
        assert pos <= len(data)
        return (RespBulkString(bulk_str), pos)


class RdbFile(RespDataType):
    def __init__(self, data: bytes):
        self.data = b64decode(
            "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="
        )

    def __len__(self) -> int:
        return len(self.data)

    def __str__(self) -> str:
        return str(self.data)

    def __repr__(self) -> str:
        return f"RdbFile({repr(self.data)})"

    def encode(self) -> bytes:
        return f"${len(self.data)}\r\n".encode() + self.data

    @staticmethod
    def decode(data: bytes, pos: int) -> tuple["RdbFile", int]:
        start = pos + 1
        while pos < len(data) and not codec.is_sep(data, pos):
            pos += 1
        if pos >= len(data):
            print("Invalid RDB file, missing \\r\\n separator")
        bulk_str_len = int(data[start:pos])
        pos += 2

        bulk_str = data[pos : pos + bulk_str_len]
        pos += bulk_str_len
        assert pos <= len(data)
        return (RdbFile(bulk_str), pos)


def decode_bulk_string_or_rdb(data: bytes, pos: int) -> tuple[RespDataType, int]:
    # check if the length ends with a sep
    orig = pos
    start = pos + 1
    while pos < len(data) and not codec.is_sep(data, pos):
        pos += 1
    # print(
    #     f"{data=}, {start=}, {pos=}, {data[start:]=}, {data[pos:]=}, {data[start:pos]=}"
    # )
    if pos >= len(data):
        print("Invalid bulk string/RDB file, missing \\r\\n separator")
    bulk_str_len = int(data[start:pos])
    pos += 2 + bulk_str_len
    if codec.is_sep(data, pos):
        return RespBulkString.decode(data, orig)
    else:
        return RdbFile.decode(data, orig)
