import bisect
from collections import defaultdict, deque
from datetime import datetime
from enum import Enum
import functools
from threading import Lock, Semaphore
import time
from typing import cast
import os

import interfaces
import constants
import data_types
from logs import logger
import rdb
import singleton_meta


class Database(metaclass=singleton_meta.SingletonMeta):
    StoreStrValType = tuple[str, datetime | None]
    StoreStreamValType = list[tuple["StreamId", dict[str, str]]]
    ConnIdType = tuple[int, str]

    class ValType(Enum):
        NONE = 0
        STRING = 1
        STREAM = 2
        LIST = 3

        def __str__(self) -> str:
            match self.value:
                case 1:
                    return "string"
                case 2:
                    return "stream"
                case 3:
                    return "list"
            return "none"

    key_types: dict[str, ValType] = {}
    store: dict[str, StoreStrValType | StoreStreamValType] = {}
    list_store: dict[str, list[bytes]] = {}  # TODO: standardise value type to bytes
    blpop_waitlist: defaultdict[str, tuple[Lock, deque[Semaphore]]] = defaultdict(
        lambda: (Lock(), deque())
    )
    xacts: dict[ConnIdType, list] = {}

    def __init__(self, dir: str, dbfilename: str):
        self.dir = dir
        self.dbfilename = dbfilename
        file_path = os.path.join(self.dir, self.dbfilename)
        if os.path.exists(file_path):
            with open(file_path, "rb") as f:
                self.rdb = rdb.RdbFile(f.read())
        else:
            self.rdb = rdb.RdbFile(constants.EMPTY_RDB_FILE)
        for key, value in self.rdb.key_values.items():
            self.store[key] = value
            self.key_types[key] = Database.ValType.STRING  # only support strings in RDB
        logger.info(f"db initialised with {self.store=}")

    def __len__(self) -> int:
        return len(self.store) + len(self.list_store)

    def __getitem__(self, key: str) -> str | StoreStreamValType | None:
        """Only returns the value, not the expiry"""
        if key not in self.store:
            return None
        key_type = self.key_types[key]
        value = self.store[key]
        match key_type:
            case Database.ValType.STRING:
                (str_val, expiry) = cast(Database.StoreStrValType, value)
                if expiry and self.expire_one(key):
                    return None
                return str_val
            case Database.ValType.LIST:
                list_val = cast(Database.StoreStreamValType, value)
                return list_val
            case Database.ValType.STREAM:
                stream_val = cast(Database.StoreStreamValType, value)
                return stream_val

    # TODO: remove? can't reliably set self.keys with this
    def __setitem__(self, key: str, value: StoreStrValType):
        self.key_types[key] = Database.ValType.STRING
        self.store[key] = value

    def __delitem__(self, key: str):
        del self.store[key]

    def __contains__(self, key: str) -> bool:
        return key in self.store

    def __str__(self) -> str:
        return str(self.store)

    def __repr__(self) -> str:
        return f"Store({repr(self.store)})"

    def get_type(self, key: str) -> str:
        if key not in self.store:
            return "none"
        # value = self.store[key]
        # if isinstance(value, list):
        #     return "stream"
        # return "string"
        return str(self.key_types[key])

    def get_expiry(self, key: str) -> datetime | None:
        if key not in self.store:
            return None
        value = self.store[key]
        # get_expiry only works for non stream values
        if not isinstance(value, tuple):
            raise Exception(f"Called get_expiry on a stream key {key=}")
        return value[1]

    def expire(self):
        for key, value in self.store.items():
            if not isinstance(value, list):
                _, expiry = value
                if expiry and expiry < datetime.now():
                    del self.store[key]

    def expire_one(self, key: str) -> bool:
        # returns True if key was expired
        value = self.store[key]
        if isinstance(value, list):
            return False
        expiry = value[1]
        if expiry and expiry < datetime.now():
            del self.store[key]
            return True
        return False

    def start_xact(self, conn_id: ConnIdType):
        self.xacts[conn_id] = []

    def xact_exists(self, conn_id: ConnIdType) -> bool:
        return conn_id in self.xacts

    def queue_xact_cmd(self, conn_id: ConnIdType, cmd: interfaces.Command):
        self.xacts[conn_id].append(cmd)

    def exec_xact(self, conn_id: ConnIdType) -> list[interfaces.Command]:
        return self.xacts.pop(conn_id)

    def rpush(self, key: str, values: list[bytes]) -> int:
        self.key_types[key] = Database.ValType.LIST
        if key not in self.list_store:
            self.list_store[key] = []
        self.list_store[key].extend(values)
        self.list_notify_queue(key)
        return len(self.list_store[key])

    def lpush(self, key: str, values: list[bytes]) -> int:
        self.key_types[key] = Database.ValType.LIST
        if key not in self.list_store:
            self.list_store[key] = []
        self.list_store[key] = values + self.list_store[key]
        self.list_notify_queue(key)
        return len(self.list_store[key])

    def list_notify_queue(self, key: str):
        queue_lock, queue = self.blpop_waitlist[key]
        with queue_lock:
            if queue:
                queue[0].release()

    def lpop(self, key: str) -> bytes:
        return self.list_store[key].pop(0)

    def lpop_multiple(self, key: str, count: int) -> list[bytes]:
        values = [self.lpop(key) for _ in range(count)]
        return values

    def blpop_timeout(self, key: str, timeout: float | None) -> bytes | None:
        """Blocks indefinitely until the list is nonempty"""
        queue_lock, queue = self.blpop_waitlist[key]
        with queue_lock:
            if key in self.list_store and len(self.list_store[key]) != 0:
                return self.lpop(key)
            else:
                sem = Semaphore(0)
                queue.append(sem)
                queue_lock.release()
                acquire_res = sem.acquire(timeout=timeout)
                queue_lock.acquire()
                if acquire_res:
                    # we succesfully acquired
                    # if the thread is notified, it must have been the leftmost in the queue
                    queue.popleft()
                    return self.lpop(key)
                else:
                    return None

    def get_list(self, key: str) -> list[bytes]:
        return self.list_store[key]

    def key_exists(self, key: str) -> bool:
        return key in self.store or key in self.list_store

    def validate_stream_id(self, key: str, id: str) -> bytes | None:
        """Returns the error when validating the stream ID, if it exists"""
        if key not in self.store:
            return None
        key_type = self.key_types[key]
        value = self.store[key]
        if key_type != Database.ValType.STREAM:
            # should change this error message
            return constants.STREAM_ID_NOT_GREATER_ERROR.encode()
        value = cast(Database.StoreStreamValType, value)

        if id == "*":
            return None
        splitted = id.split("-")
        if len(splitted) != 2:
            # should change this one too
            return constants.STREAM_ID_NOT_GREATER_ERROR.encode()
        _, seq_no = splitted
        seq_no_is_star = seq_no == "*"
        stream_id = StreamId(id)
        if seq_no_is_star:
            return None
        is_0_0 = stream_id.milliseconds_time == "0" and stream_id.seq_no == "0"
        if is_0_0:
            return constants.STREAM_ID_TOO_SMALL_ERROR.encode()

        if not value:
            return None
        last_stream_id = value[-1][0]
        if stream_id <= last_stream_id:
            return constants.STREAM_ID_NOT_GREATER_ERROR.encode()
        return None

    def xadd(self, key: str, id: str, value: dict) -> str:
        # stream key has already been validated
        self.key_types[key] = Database.ValType.STREAM
        if key not in self.store:
            self.store[key] = []
        cur_value = self.store[key]
        if not isinstance(cur_value, list):
            raise Exception(f"key {key} is not a stream")
        processed_id = StreamId.generate_stream_id(
            id, cur_value[-1][0] if cur_value else None
        )
        cur_value.append((processed_id, value))
        return str(processed_id)

    def xrange(self, key: str, start: str, end: str) -> bytes:
        value = self.store[key]
        key_type = self.key_types[key]
        if key_type != Database.ValType.STREAM:
            return constants.XOP_ON_NON_STREAM_ERROR.encode()
        value = cast(Database.StoreStreamValType, value)

        # support - and + queries
        if start == "-":
            start_stream_id = StreamId("0-1")
        elif "-" not in start:
            start_stream_id = StreamId(f"{start}-0")
        else:
            start_stream_id = StreamId(start)
        if end == "+":
            end_stream_id = (
                value[-1][0]
                if value
                else StreamId(
                    f"{constants.MAX_STREAM_ID_SEQ_NO}-{constants.MAX_STREAM_ID_SEQ_NO}"
                )
            )
        elif "-" not in end:
            end_stream_id = StreamId(f"{end}-{constants.MAX_STREAM_ID_SEQ_NO}")
        else:
            end_stream_id = StreamId(end)

        lo = bisect.bisect_right(value, start_stream_id, key=lambda x: x[0])
        if lo >= len(value):
            return data_types.RespArray([]).encode()
        hi = bisect.bisect_right(value, end_stream_id, key=lambda x: x[0])
        if hi >= len(value):
            hi = len(value)

        res = []
        for i in range(lo - 1 if lo != 0 else 0, hi):
            flattened_kvs = []
            for k, v in value[i][1].items():
                flattened_kvs.append(data_types.RespBulkString(k.encode()))
                flattened_kvs.append(data_types.RespBulkString(v.encode()))
            res.append(
                data_types.RespArray(
                    [
                        data_types.RespBulkString(str(value[i][0]).encode()),
                        data_types.RespArray(flattened_kvs),
                    ]
                )
            )
        return data_types.RespArray(res).encode()

    def xread(
        self, stream_keys: list[str], ids: list[str], timeout: int | None
    ) -> bytes:
        if timeout is not None:
            original_lens = [len(self.store[stream_key]) for stream_key in stream_keys]
            logger.info(f"{original_lens=}")
            if timeout != 0:
                time.sleep(timeout / 1e3)
            else:
                while True:
                    time.sleep(0.5)
                    new_lens = [
                        len(self.store[stream_key]) for stream_key in stream_keys
                    ]
                    to_break = False
                    for i in range(len(original_lens)):
                        if new_lens[i] != original_lens[i]:
                            to_break = True
                    if to_break:
                        logger.info(f"{new_lens=}")
                        break

        res = []
        for i in range(len(stream_keys)):
            stream_key = stream_keys[i]
            id = ids[i]
            value = self.store[stream_key]
            key_type = self.key_types[stream_key]
            if key_type != Database.ValType.STREAM:
                return constants.XOP_ON_NON_STREAM_ERROR.encode()
            value = cast(Database.StoreStreamValType, value)
            if id == "$":
                logger.info(f"{original_lens[i]=}")
                id = str(value[original_lens[i] - 1][0]) if value else "0-0"
            stream_id = StreamId(id)

            lo = bisect.bisect_right(value, stream_id, key=lambda x: x[0])
            if lo >= len(value):
                return constants.NULL_BULK_RESP_STRING.encode()

            inter: list[data_types.RespDataType] = []
            for i in range(lo, len(value)):
                flattened_kvs = []
                for k, v in value[i][1].items():
                    flattened_kvs.append(data_types.RespBulkString(k.encode()))
                    flattened_kvs.append(data_types.RespBulkString(v.encode()))
                inter.append(
                    data_types.RespArray(
                        [
                            data_types.RespBulkString(str(value[i][0]).encode()),
                            data_types.RespArray(flattened_kvs),
                        ]
                    )
                )
            res.append(
                data_types.RespArray(
                    [
                        data_types.RespBulkString(stream_key.encode()),
                        data_types.RespArray(inter),
                    ]
                )
            )
        return data_types.RespArray(res).encode()


@functools.total_ordering
class StreamId:
    """ID of a stream entry"""

    def __init__(self, id_str: str):
        milliseconds_time, seq_no = id_str.split("-")
        self.milliseconds_time = milliseconds_time
        self.seq_no = seq_no

    def __repr__(self) -> str:
        return f"StreamId({self.milliseconds_time}-{self.seq_no})"

    def __str__(self) -> str:
        return f"{self.milliseconds_time}-{self.seq_no}"

    def __eq__(self, other) -> bool:
        if not isinstance(other, StreamId):
            return False
        return (
            self.milliseconds_time == other.milliseconds_time
            and self.seq_no == other.seq_no
        )

    def __lt__(self, other: "StreamId"):
        if self.milliseconds_time != other.milliseconds_time:
            return self.milliseconds_time < other.milliseconds_time
        return self.seq_no < other.seq_no

    @staticmethod
    def generate_stream_id(id: str, last_id: "StreamId | None") -> "StreamId":
        if id == "*":
            # milliseconds_time should be current time in milliseconds
            milliseconds_time = str(int(datetime.now().timestamp() * 1000))
            if not last_id:
                return StreamId(f"{milliseconds_time}-0")
            if last_id.milliseconds_time == milliseconds_time:
                return last_id.next_seq_id()
            return StreamId(f"{milliseconds_time}-0")

        splitted = id.split("-")
        if len(splitted) != 2:
            raise Exception(f"Invalid stream id {id}")
        milliseconds_time, seq_no = splitted
        if not last_id:
            if seq_no == "*":
                seq_no = "1" if milliseconds_time == "0" else "0"
            return StreamId(f"{milliseconds_time}-{seq_no}")

        if seq_no == "*":
            if milliseconds_time == last_id.milliseconds_time:
                seq_no = str(int(last_id.seq_no) + 1)
            else:
                seq_no = "1" if milliseconds_time == "0" else "0"
        return StreamId(f"{milliseconds_time}-{seq_no}")

    def next_seq_id(self) -> "StreamId":
        return StreamId(f"{self.milliseconds_time}-{int(self.seq_no) + 1}")
