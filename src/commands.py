from datetime import datetime, timedelta
from typing import Iterable

import constants
from data_types import (
    RespSimpleString,
    RespBulkString,
    RespArray,
    RespInteger,
    RespPlainString,
    RespSimpleError,
    RespRdbFile,
)
import exceptions
from interfaces import Command
from logs import logger
from utils import construct_conn_id
import replicas


class NoOp(Command):
    expected_arg_count = [0]

    def __init__(self, raw_cmd: bytes):
        self._raw_cmd = raw_cmd
        self._keyword = b"NOOP"

    def execute(self, db, replica_handler, conn) -> bytes:
        return constants.NO_OP_ERROR.encode()

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return NoOp(b"")


class PingCommand(Command):
    expected_arg_count = [0]

    def __init__(self, raw_cmd: bytes):
        self._raw_cmd = raw_cmd
        self._keyword = b"PING"
        self.in_subscribed_mode = False

    def execute(self, db, replica_handler, conn) -> bytes:
        logger.info(f"{self.in_subscribed_mode=}")
        if self.in_subscribed_mode:
            return RespArray([RespSimpleString(b"pong"), RespBulkString(b"")]).encode()
        else:
            return RespSimpleString(b"PONG").encode()

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return PingCommand(craft_command("PING").encode())


class EchoCommand(Command):
    expected_arg_count = [1]

    def __init__(self, raw_cmd: bytes, bulk_str: RespBulkString):
        self._raw_cmd = raw_cmd
        self.msg = bulk_str.data
        self._keyword = b"ECHO"

    def execute(self, db, replica_handler, conn) -> bytes:
        return RespSimpleString(self.msg).encode()

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return EchoCommand(
            craft_command("ECHO", *args).encode(),
            RespBulkString(args[0].encode()),
        )


class SetCommand(Command):
    expected_arg_count = [2, 3]

    def __init__(
        self,
        raw_cmd: bytes,
        key: RespBulkString,
        value: RespBulkString,
        expiry: RespBulkString | None,
    ):
        self._raw_cmd = raw_cmd
        self.key = key.data
        self.value = value.data
        self.expiry = (
            (datetime.now() + timedelta(milliseconds=int(expiry.data)))
            if expiry
            else None
        )
        self._keyword = b"SET"

    def execute(self, db, replica_handler: replicas.ReplicaHandler, conn) -> bytes:
        replica_handler.propogate(self._raw_cmd)
        db[self.key.decode()] = (self.value.decode(), self.expiry)
        return constants.OK_SIMPLE_RESP_STRING.encode()

    @staticmethod
    def validate_px(px_cmd: RespBulkString):
        if px_cmd.data.upper() != b"PX":
            raise exceptions.ValidationError(
                f"Unsupported SET command (fourth element is not 'PX') {px_cmd.data}"
            )

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return SetCommand(
            craft_command("SET", *args).encode(),
            RespBulkString(args[0].encode()),
            RespBulkString(args[1].encode()),
            RespBulkString(args[2].encode()) if len(args) > 2 else None,
        )


class IncrCommand(Command):
    expected_arg_count = [1]

    def __init__(
        self,
        raw_cmd: bytes,
        key: bytes,
    ):
        self._raw_cmd = raw_cmd
        self.key = key
        self._keyword = b"INCR"

    def execute(self, db, replica_handler: replicas.ReplicaHandler, conn) -> bytes:
        decoded_key = self.key.decode()
        old_value = db[decoded_key]
        # TODO: use key_types
        # assume that old_value is always a str
        if isinstance(old_value, list):
            raise exceptions.UnsupportedOperationError(
                "INCR command is unsupported for stream values"
            )
        if old_value:
            try:
                new_value = str(int(old_value) + 1)
            except ValueError:
                return RespSimpleError(
                    b"ERR value is not an integer or out of range"
                ).encode()
            expiry = db.get_expiry(decoded_key)
        else:
            new_value = str(1)
            expiry = None

        db[decoded_key] = (new_value, expiry)
        return RespInteger(int(new_value)).encode()

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return IncrCommand(craft_command("INCR", *args).encode(), args[0].encode())


class GetCommand(Command):
    expected_arg_count = [1]

    def __init__(self, raw_cmd: bytes, key: bytes):
        self._raw_cmd = raw_cmd
        self.key = key
        self._keyword = b"GET"

    def execute(self, db, replica_handler, conn) -> bytes:
        if self.key.decode() in db:
            value = db[self.key.decode()]
            if isinstance(value, str):
                return RespBulkString(value.encode()).encode()
            elif isinstance(value, list):
                return RespBulkString(str(value).encode()).encode()
        return constants.NULL_BULK_RESP_STRING.encode()

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return GetCommand(
            craft_command("GET", *args).encode(),
            args[0].encode(),
        )


class CommandCommand(Command):
    expected_arg_count = [0]

    def __init__(self, raw_cmd: bytes):
        self._raw_cmd = raw_cmd
        self._keyword = b"COMMAND"

    def execute(self, db, replica_handler, conn) -> bytes:
        return constants.OK_SIMPLE_RESP_STRING.encode()

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return CommandCommand(craft_command("COMMAND").encode())


class InfoCommand(Command):
    expected_arg_count = [0]

    def __init__(self, raw_cmd: bytes):
        self._raw_cmd = raw_cmd
        self._keyword = b"INFO"

    def execute(self, db, replica_handler: replicas.ReplicaHandler, conn) -> bytes:
        return replica_handler.get_info()

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return InfoCommand(craft_command("INFO").encode())


class ReplConfCommand(Command):
    expected_arg_count = [0]

    def __init__(self, raw_cmd: bytes):
        self._raw_cmd = raw_cmd
        self._keyword = b"REPLCONF"

    def execute(self, db, replica_handler, conn) -> bytes:
        return constants.OK_SIMPLE_RESP_STRING.encode()

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return ReplConfCommand(craft_command("REPLCONF").encode())


class ReplConfAckCommand(Command):
    expected_arg_count = [1]

    def __init__(self, raw_cmd: bytes):
        self._raw_cmd = raw_cmd
        self._keyword = b"REPLCONF"

    def execute(self, db, replica_handler: replicas.ReplicaHandler, conn) -> bytes:
        logger.info(
            f"incrementing {replica_handler.ack_count=} to {replica_handler.ack_count + 1}"
        )
        replica_handler.ack_count += 1
        return b""

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return ReplConfAckCommand(craft_command("REPLCONF", "ACK", args[0]).encode())


class ReplConfGetAckCommand(Command):
    expected_arg_count = [0]

    def __init__(self, raw_cmd: bytes):
        self._raw_cmd = raw_cmd
        self._keyword = b"REPLCONF"

    def execute(self, db, replica_handler: replicas.ReplicaHandler, conn) -> bytes:
        replica_handler.propogate(self._raw_cmd)
        return RespArray(
            [
                RespBulkString(b"REPLCONF"),
                RespBulkString(b"ACK"),
                RespBulkString(str(replica_handler.master_repl_offset).encode()),
            ]
        ).encode()

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return ReplConfGetAckCommand(craft_command("REPLCONF", "GETACK").encode())


class PsyncCommand(Command):
    expected_arg_count = [0]

    def __init__(self, raw_cmd: bytes):
        self._raw_cmd = raw_cmd
        self._keyword = b"FULLRESYNC"

    def execute(
        self, db, replica_handler: replicas.ReplicaHandler, conn
    ) -> list[bytes]:
        replica_handler.add_slave(conn)
        return [
            RespSimpleString(
                f"FULLRESYNC {replica_handler.ip} {replica_handler.master_repl_offset}".encode()
            ).encode(),
            RespRdbFile(constants.EMPTY_RDB_FILE).encode(),
        ]

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return PsyncCommand(craft_command("PSYNC").encode())


class FullResyncCommand(Command):
    expected_arg_count = [1]

    def __init__(self, data: bytes) -> None:
        self.data = data
        self._raw_cmd = data
        self._keyword = b"FULLRESYNC"

    def execute(self, db, replica_handler, conn) -> bytes:
        return b""

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return FullResyncCommand(craft_command("FULLRESYNC").encode())


class RdbFileCommand(Command):
    expected_arg_count = [0]

    def __init__(self, data: bytes) -> None:
        self.rdbfile = RespRdbFile(data)
        self._raw_cmd = data
        self._keyword = b""

    # slave received a RDB file
    def execute(self, db, replica_handler: replicas.ReplicaHandler, conn) -> bytes:
        return b""

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return RdbFileCommand(constants.EMPTY_RDB_FILE)


class ConfigGetCommand(Command):
    expected_arg_count = [1]

    def __init__(self, raw_cmd: bytes, key: bytes):
        self._raw_cmd = raw_cmd
        self.key = key
        self._keyword = b"CONFIG"

    def execute(self, db, replica_handler, conn) -> bytes:
        if self.key.upper() == b"DIR":
            return RespArray(
                [
                    RespBulkString(self.key),
                    RespBulkString(db.dir.encode()),
                ]
            ).encode()
        elif self.key.upper() == b"DBFILENAME":
            return RespArray(
                [
                    RespBulkString(self.key),
                    RespBulkString(db.dbfilename.encode()),
                ]
            ).encode()
        return constants.OK_SIMPLE_RESP_STRING.encode()

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return ConfigGetCommand(
            craft_command("CONFIG", "GET", args[0]).encode(), args[0].encode()
        )


class KeysCommand(Command):
    expected_arg_count = [1]

    def __init__(self, raw_cmd: bytes, pattern: bytes):
        self._raw_cmd = raw_cmd
        self.pattern = pattern
        self._keyword = b"KEYS"

    def execute(self, db, replica_handler, conn) -> bytes:
        return RespArray(
            list(
                map(
                    lambda x: RespBulkString(x.encode()),
                    db.rdb.key_values.keys(),
                )
            )
        ).encode()

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return KeysCommand(craft_command("KEYS", args[0]).encode(), args[0].encode())


class WaitCommand(Command):
    expected_arg_count = [2]

    def __init__(self, raw_cmd: bytes, replica_count: int, timeout: int):
        self._raw_cmd = raw_cmd
        self.replica_count = replica_count
        self.timeout = timedelta(milliseconds=timeout)
        self._keyword = b"WAIT"

    def execute(self, db, replica_handler: replicas.ReplicaHandler, conn) -> bytes:
        now = datetime.now()
        end = now + self.timeout
        replica_handler.ack_count = 0
        replica_handler.propogate(
            RespArray(
                [
                    RespBulkString(b"REPLCONF"),
                    RespBulkString(b"GETACK"),
                    RespBulkString(b"*"),
                ]
            ).encode()
        )
        logger.info(f"finished sending to all slaves")
        while replica_handler.ack_count < self.replica_count and datetime.now() < end:
            pass

        logger.info(
            f"{replica_handler.ack_count=}, {datetime.now() - end=} (should be positive)"
        )
        # hardcode to len(slaves) if no acks
        return RespInteger(
            replica_handler.ack_count
            if replica_handler.ack_count > 0
            else len(replica_handler.slaves)
        ).encode()

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return WaitCommand(
            craft_command("WAIT", *args).encode(), int(args[0]), int(args[1])
        )


class TypeCommand(Command):
    expected_arg_count = [1]

    def __init__(self, raw_cmd: bytes, key: bytes):
        self._raw_cmd = raw_cmd
        self.key = key
        self._keyword = b"TYPE"

    def execute(self, db, replica_handler, conn) -> bytes:
        if self.key.decode() in db:
            return RespSimpleString(db.get_type(self.key.decode()).encode()).encode()
        return RespSimpleString(b"none").encode()

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return TypeCommand(
            craft_command("TYPE", *args).encode(),
            args[0].encode(),
        )


class MultiCommand(Command):
    expected_arg_count = [0]

    def __init__(self, raw_cmd: bytes):
        self._raw_cmd = raw_cmd
        self._keyword = b"MULTI"

    def execute(self, db, replica_handler, conn) -> bytes:
        conn_id = construct_conn_id(conn)
        db.start_xact(conn_id)
        return constants.OK_SIMPLE_RESP_STRING.encode()

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return MultiCommand(craft_command("MULTI", *args).encode())


class ExecCommand(Command):
    expected_arg_count = [0]

    def __init__(self, raw_cmd: bytes):
        self._raw_cmd = raw_cmd
        self._keyword = b"EXEC"

    def execute(self, db, replica_handler, conn) -> bytes:
        conn_id = construct_conn_id(conn)
        if not db.xact_exists(conn_id):
            return RespSimpleError(b"ERR EXEC without MULTI").encode()
        cmds = db.exec_xact(conn_id)
        responses = [cmd.execute(db, replica_handler, conn) for cmd in cmds]
        flattened = []
        for response in responses:
            if isinstance(response, list):
                for inner_response in response:
                    flattened.append(RespPlainString(inner_response))
            else:
                flattened.append(RespPlainString(response))
        return RespArray(flattened).encode()

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return ExecCommand(craft_command("EXEC", *args).encode())


class DiscardCommand(Command):
    expected_arg_count = [0]

    def __init__(self, raw_cmd: bytes):
        self._raw_cmd = raw_cmd
        self._keyword = b"DISCARD"

    def execute(self, db, replica_handler, conn) -> bytes:
        conn_id = construct_conn_id(conn)
        if not db.xact_exists(conn_id):
            return RespSimpleError(b"ERR DISCARD without MULTI").encode()
        db.exec_xact(conn_id)
        return constants.OK_SIMPLE_RESP_STRING.encode()

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return DiscardCommand(craft_command("DISCARD", *args).encode())


class RpushCommand(Command):
    expected_arg_count = [2]

    def __init__(
        self,
        raw_cmd: bytes,
        key: bytes,
        values: list[bytes],
    ):
        self._raw_cmd = raw_cmd
        self.key = key
        self.values = values
        self._keyword = b"RPUSH"

    def execute(self, db, replica_handler, conn) -> bytes:
        length = db.rpush(self.key.decode(), self.values)
        return RespInteger(length).encode()

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return RpushCommand(
            craft_command("RPUSH", *args).encode(),
            args[0].encode(),
            [arg.encode() for arg in args],
        )


class LpushCommand(Command):
    expected_arg_count = [2]

    def __init__(
        self,
        raw_cmd: bytes,
        key: bytes,
        values: list[bytes],
    ):
        self._raw_cmd = raw_cmd
        self.key = key
        self.values = values
        self._keyword = b"LPUSH"

    def execute(self, db, replica_handler, conn) -> bytes:
        length = db.lpush(self.key.decode(), self.values[::-1])
        return RespInteger(length).encode()

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return LpushCommand(
            craft_command("LPUSH", *args).encode(),
            args[0].encode(),
            [arg.encode() for arg in args],
        )


class LpopCommand(Command):
    expected_arg_count = [1, 2]

    def __init__(self, raw_cmd: bytes, key: bytes, count: int):
        self._raw_cmd = raw_cmd
        self.key = key
        self.count = count
        self._keyword = b"LPOP"

    def execute(self, db, replica_handler, conn) -> bytes:
        if self.count == 1:
            value = db.lpop(self.key.decode())
            return RespBulkString(value).encode()
        else:
            values = db.lpop_multiple(self.key.decode(), self.count)
            return RespArray([RespBulkString(value) for value in values]).encode()

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return LpopCommand(
            craft_command("LPOP", *args).encode(), args[0].encode(), int(args[1])
        )


class BlpopCommand(Command):
    expected_arg_count = [1, 2]

    def __init__(self, raw_cmd: bytes, key: bytes, timeout: float):
        self._raw_cmd = raw_cmd
        self.key = key
        self.timeout = timeout
        self._keyword = b"BLPOP"

    def execute(self, db, replica_handler, conn) -> bytes:
        value = db.blpop_timeout(
            self.key.decode(), None if self.timeout == 0 else self.timeout
        )
        if value:
            return RespArray([RespBulkString(self.key), RespBulkString(value)]).encode()
        else:
            # timed out
            return constants.NULL_BULK_RESP_STRING.encode()

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return BlpopCommand(
            craft_command("LPOP", *args).encode(),
            args[0].encode(),
            int(args[1]) if len(args) > 1 else 0,
        )


class LlenCommand(Command):
    expected_arg_count = [1]

    def __init__(
        self,
        raw_cmd: bytes,
        key: bytes,
    ):
        self._raw_cmd = raw_cmd
        self.key = key
        self._keyword = b"LLEN"

    def execute(self, db, replica_handler, conn) -> bytes:
        if not db.key_exists(self.key.decode()):
            length = 0
        else:
            length = len(db.get_list(self.key.decode()))
        return RespInteger(length).encode()

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return LlenCommand(
            craft_command("LLEN", *args).encode(),
            args[0].encode(),
        )


class LrangeCommand(Command):
    expected_arg_count = [3]

    def __init__(self, raw_cmd: bytes, key: bytes, start: int, stop: int):
        self._raw_cmd = raw_cmd
        self.key = key.decode()
        self.start = start
        self.stop = stop
        self._keyword = b"LRANGE"

    def execute(self, db, replica_handler, conn) -> bytes:
        if not db.key_exists(self.key) or (self.stop >= 0 and self.start > self.stop):
            return constants.EMPTY_RESP_ARRAY.encode()
        retrieved_list = db.get_list(self.key)
        if self.start >= len(retrieved_list):
            return constants.EMPTY_RESP_ARRAY.encode()
        # automatically handles stop being larger than array
        if self.stop == -1:
            adjusted_stop = len(retrieved_list)
        else:
            adjusted_stop = self.stop + 1
        return RespArray(
            [RespBulkString(val) for val in retrieved_list[self.start : adjusted_stop]]
        ).encode()

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return LrangeCommand(
            craft_command("LRANGE", *args).encode(),
            args[0].encode(),
            int(args[1]),
            int(args[2]),
        )


class XaddCommand(Command):
    def __init__(self, raw_cmd: bytes, stream_key: bytes, data: list[RespBulkString]):
        self._raw_cmd = raw_cmd
        self.stream_key = stream_key
        self.data = data
        self._keyword = b"XADD"

    def execute(
        self,
        db,
        replica_handler,
        conn,
    ) -> bytes:
        raw_stream_entry_id = self.data[0]
        stream_entry_id = raw_stream_entry_id.data
        err = db.validate_stream_id(self.stream_key.decode(), stream_entry_id.decode())
        if err is not None:
            return RespSimpleError(err).encode()

        kv_dict = {}
        for i in range(1, len(self.data), 2):
            stream_key = self.data[i]
            stream_value = self.data[i + 1]
            kv_dict[stream_key.data.decode()] = stream_value.data.decode()
        logger.info(f"{stream_entry_id=}, {kv_dict=}")
        processed_stream_id = db.xadd(
            self.stream_key.decode(), stream_entry_id.decode(), kv_dict
        )
        return RespBulkString(processed_stream_id.encode()).encode()

    @classmethod
    def craft_request(cls, *args: str):
        args_len = len(args)
        if args_len < 2 or args_len % 2 != 0:
            error_msg = f"{cls.__name__} takes an even number of argument(s), but {args_len} {'was' if args_len == 1 else 'were'} provided"
            raise exceptions.RequestCraftError(error_msg)

        return XaddCommand(
            craft_command("XADD", *args).encode(),
            args[0].encode(),
            list(map(lambda x: RespBulkString(x.encode()), args[1:])),
        )


class XrangeCommand(Command):
    expected_arg_count = [3]

    def __init__(self, raw_cmd: bytes, key: bytes, start: str, end: str):
        self._raw_cmd = raw_cmd
        self.key = key
        self.start = start
        self.end = end
        self._keyword = b"XRANGE"

    def execute(self, db, replica_handler, conn) -> bytes:
        return db.xrange(self.key.decode(), self.start, self.end)

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return XrangeCommand(
            craft_command("XRANGE", *args).encode(), args[0].encode(), args[1], args[2]
        )


class XreadCommand(Command):
    def __init__(
        self,
        raw_cmd: bytes,
        stream_keys: list[str],
        ids: list[str],
        timeout: int | None = None,
    ):
        self._raw_cmd = raw_cmd
        self.stream_keys = stream_keys
        self.ids = ids
        self.timeout = timeout
        self._keyword = b"XREAD"

    def execute(self, db, replica_handler, conn) -> bytes:
        res = db.xread(self.stream_keys, self.ids, self.timeout)
        return res

    @classmethod
    def craft_request(cls, *args: str):
        if args[0].upper() == "BLOCK":
            verify_arg_count(cls.__name__, [4], len(args))
            return XreadCommand(
                craft_command("XREAD", *args).encode(),
                list(args[2:]),
                list(args[1:2]),
                int(args[3]),
            )
        else:
            verify_arg_count(cls.__name__, [2], len(args))
            return XreadCommand(
                craft_command("XREAD", *args).encode(), list(args[1:]), list(args[0])
            )


class SubscribeCommand(Command):
    expected_arg_count = [1]

    def __init__(self, raw_cmd: bytes, channel_name: bytes):
        self._raw_cmd = raw_cmd
        self.channel_name = channel_name
        self._keyword = b"SUBSCRIBE"

    def execute(self, db, replica_handler, conn) -> bytes:
        conn_id = construct_conn_id(conn)
        channel_count = db.subscribe(self.channel_name.decode(), conn, conn_id)
        return RespArray(
            [
                RespBulkString(b"subscribe"),
                RespBulkString(self.channel_name),
                RespInteger(channel_count),
            ]
        ).encode()

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return SubscribeCommand(
            craft_command("SUBSCRIBE", *args).encode(), args[0].encode()
        )


class UnsubscribeCommand(Command):
    expected_arg_count = [1]

    def __init__(self, raw_cmd: bytes, channel_name: bytes):
        self._raw_cmd = raw_cmd
        self.channel_name = channel_name
        self._keyword = b"UNSUBSCRIBE"

    def execute(self, db, replica_handler, conn) -> bytes:
        conn_id = construct_conn_id(conn)
        channel_count = db.unsubscribe(self.channel_name.decode(), conn, conn_id)
        return RespArray(
            [
                RespBulkString(b"unsubscribe"),
                RespBulkString(self.channel_name),
                RespInteger(channel_count),
            ]
        ).encode()

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return UnsubscribeCommand(
            craft_command("UNSUBSCRIBE", *args).encode(), args[0].encode()
        )


class PublishCommand(Command):
    expected_arg_count = [2]

    def __init__(self, raw_cmd: bytes, channel_name: bytes, msg: bytes):
        self._raw_cmd = raw_cmd
        self.channel_name = channel_name
        self.msg = msg
        self._keyword = b"PUBLISH"

    def execute(self, db, replica_handler, conn) -> bytes:
        publish_msg = RespArray(
            [
                RespBulkString(b"message"),
                RespBulkString(self.channel_name),
                RespBulkString(self.msg),
            ]
        ).encode()
        for _, subscribed_conn in db.get_subscribers(self.channel_name.decode()):
            subscribed_conn.sendall(publish_msg)
        return RespInteger(len(db.get_subscribers(self.channel_name.decode()))).encode()

    @classmethod
    def craft_request(cls, *args: str):
        verify_arg_count(cls.__name__, cls.expected_arg_count, len(args))
        return PublishCommand(
            craft_command("PUBLISH", *args).encode(), args[0].encode(), args[1].encode()
        )


def verify_arg_count(
    command_name: str, expected_arg_count: Iterable[int], args_len: int
):
    """Raises a RequestCraftError if arg count does not match"""
    if args_len not in expected_arg_count:
        error_msg = f"{command_name} takes {'/'.join(str(i) for i in expected_arg_count)} argument(s), but {args_len} {'was' if args_len == 1 else 'were'} provided"
        raise exceptions.RequestCraftError(error_msg)


def craft_command(*args: str) -> RespArray:
    return RespArray(list(map(lambda x: RespBulkString(x.encode()), args)))
