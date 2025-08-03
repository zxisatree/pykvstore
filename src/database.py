import bisect
from collections import defaultdict, deque
from datetime import datetime
from enum import Enum
import functools
import os
import socket
from threading import Lock, RLock, Semaphore
import time
from typing import cast

import interfaces
import constants
from data_types import RespArray, RespBulkString, RespDataType
from logs import logger
import rdb
import singleton_meta


class ThreadsafeDict[KT, VT](dict):
    def __init__(self, *args, **kwargs):
        self.lock = Lock()
        super().__init__(*args, **kwargs)

    def __getitem__(self, key: KT) -> VT:
        with self.lock:
            return super().__getitem__(key)

    def __setitem__(self, key: KT, value: VT):
        with self.lock:
            return super().__setitem__(key, value)

    def __contains__(self, key: KT) -> bool:
        with self.lock:
            return super().__contains__(key)

    def __len__(self) -> int:
        with self.lock:
            return super().__len__()

    def __delitem__(self, key: KT):
        with self.lock:
            return super().__delitem__(key)

    def __str__(self) -> str:
        return super().__str__()

    def __repr__(self) -> str:
        return f"ThreadsafeDict{super().__repr__()}"


class ThreadsafeDefaultdict[KT, VT](defaultdict):
    def __init__(self, *args, **kwargs):
        # defaultdict might call __setitem__ in __getitem__
        self.lock = RLock()
        super().__init__(*args, **kwargs)

    def __getitem__(self, key: KT) -> VT:
        with self.lock:
            return super().__getitem__(key)

    def __setitem__(self, key: KT, value: VT):
        with self.lock:
            return super().__setitem__(key, value)

    def __contains__(self, key: KT) -> bool:
        with self.lock:
            return super().__contains__(key)

    def __len__(self) -> int:
        with self.lock:
            return super().__len__()

    def __delitem__(self, key: KT):
        with self.lock:
            return super().__delitem__(key)

    def __str__(self) -> str:
        return super().__str__()

    def __repr__(self) -> str:
        return f"ThreadsafeDefaultdict{super().__repr__()}"


class Database(metaclass=singleton_meta.SingletonMeta):
    StrVal = tuple[str, datetime | None]
    StreamVal = list[tuple["StreamId", dict[str, str]]]
    ListVal = list[bytes]
    ConnId = tuple[int, str]

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

    def __init__(self, dir: str, dbfilename: str):
        # TODO: standardise key type to bytes
        self.store: ThreadsafeDict[
            str, Database.StrVal | Database.StreamVal | Database.ListVal
        ] = ThreadsafeDict()
        self.key_types: ThreadsafeDict[str, Database.ValType] = ThreadsafeDict()
        self.blpop_waitlist: ThreadsafeDefaultdict[
            str, tuple[Lock, deque[Semaphore]]
        ] = ThreadsafeDefaultdict(lambda: (Lock(), deque()))
        self.xacts: ThreadsafeDict[Database.ConnId, list] = ThreadsafeDict()
        self.channels: ThreadsafeDefaultdict[Database.ConnId, set[str]] = (
            ThreadsafeDefaultdict(set)
        )
        self.subscribers: ThreadsafeDefaultdict[
            str, set[tuple[Database.ConnId, socket.socket]]
        ] = ThreadsafeDefaultdict(set)

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
        return len(self.store)

    def __getitem__(self, key: str) -> str | StreamVal | None:
        """Only returns the value, not the expiry"""
        if key not in self.store:
            return None
        key_type = self.key_types[key]
        value = self.store[key]
        match key_type:
            case Database.ValType.STRING:
                (str_val, expiry) = cast(Database.StrVal, value)
                if expiry and self.expire_one(key):
                    return None
                return str_val
            case Database.ValType.LIST:
                list_val = cast(Database.StreamVal, value)
                return list_val
            case Database.ValType.STREAM:
                stream_val = cast(Database.StreamVal, value)
                return stream_val

    # TODO: remove? can't reliably set self.keys with this
    def __setitem__(self, key: str, value: StrVal):
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
        return str(self.key_types[key])

    def get_expiry(self, key: str) -> datetime | None:
        if key not in self.store:
            return None
        value = self.store[key]
        # get_expiry only works for string values
        key_type = self.key_types[key]
        if key_type == Database.ValType.STRING:
            string_val = cast(Database.StrVal, value)
            return string_val[1]
        else:
            raise Exception(f"Called get_expiry on a non string key {key=}")

    def expire_one(self, key: str) -> bool:
        # returns True if key was expired
        value = self.store[key]
        key_type = self.key_types[key]
        if key_type != Database.ValType.STRING:
            return False
        _, expiry = cast(Database.StrVal, value)
        if expiry and expiry < datetime.now():
            del self.store[key]
            return True
        return False

    def start_xact(self, conn_id: ConnId):
        self.xacts[conn_id] = []

    def xact_exists(self, conn_id: ConnId) -> bool:
        return conn_id in self.xacts

    def queue_xact_cmd(self, conn_id: ConnId, cmd: interfaces.Command):
        self.xacts[conn_id].append(cmd)

    def exec_xact(self, conn_id: ConnId) -> list[interfaces.Command]:
        return self.xacts.pop(conn_id)

    def in_subscribed_mode(self, conn_id: ConnId) -> bool:
        return conn_id in self.channels

    def subscribe(self, channel_name: str, conn: socket.socket, conn_id: ConnId) -> int:
        """Subscribe to a channel, and return the number of channels the client is subscribed to"""
        self.channels[conn_id].add(channel_name)
        self.subscribers[channel_name].add((conn_id, conn))
        return len(self.channels[conn_id])

    def unsubscribe(
        self, channel_name: str, conn: socket.socket, conn_id: ConnId
    ) -> int:
        """Unubscribe from a channel, and return the number of channels the client is subscribed to"""
        if channel_name in self.channels[conn_id]:
            self.channels[conn_id].remove(channel_name)
            self.subscribers[channel_name].remove((conn_id, conn))
        return len(self.channels[conn_id])

    def get_subscribers(self, channel_name: str) -> set[tuple[ConnId, socket.socket]]:
        return self.subscribers[channel_name]

    def rpush(self, key: str, values: ListVal) -> int:
        if key not in self.store:
            self.store[key] = []
        elif key in self.store and self.key_types[key] != Database.ValType.LIST:
            raise Exception(f"Called rpush on a non list key {key=}")
        self.key_types[key] = Database.ValType.LIST
        cast(Database.ListVal, self.store[key]).extend(values)
        self.list_notify_queue(key)
        return len(self.store[key])

    def lpush(self, key: str, values: ListVal) -> int:
        if key not in self.store:
            self.store[key] = []
        elif key in self.store and self.key_types[key] != Database.ValType.LIST:
            raise Exception(f"Called lpush on a non list key {key=}")
        self.key_types[key] = Database.ValType.LIST
        self.store[key] = values + cast(Database.ListVal, self.store[key])
        self.list_notify_queue(key)
        return len(self.store[key])

    def list_notify_queue(self, key: str):
        queue_lock, queue = self.blpop_waitlist[key]
        with queue_lock:
            if queue:
                queue[0].release()

    def lpop(self, key: str) -> bytes:
        return self.get_list(key).pop(0)

    def lpop_multiple(self, key: str, count: int) -> ListVal:
        values = [self.lpop(key) for _ in range(count)]
        return values

    def blpop_timeout(self, key: str, timeout: float | None) -> bytes | None:
        """Blocks indefinitely until the list is nonempty"""
        queue_lock, queue = self.blpop_waitlist[key]
        with queue_lock:
            if key in self.store and len(self.store[key]) != 0:
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

    def get_list(self, key: str) -> ListVal:
        if self.key_types[key] != Database.ValType.LIST:
            raise Exception("Called get_list on a non list key {key=}")
        return cast(Database.ListVal, self.store[key])

    def key_exists(self, key: str) -> bool:
        return key in self.store or key in self.store

    def validate_stream_id(self, key: str, id: str) -> bytes | None:
        """Returns the error when validating the stream ID, if it exists"""
        if key not in self.store:
            return None
        key_type = self.key_types[key]
        value = self.store[key]
        if key_type != Database.ValType.STREAM:
            # should change this error message
            return constants.STREAM_ID_NOT_GREATER_ERROR.encode()
        value = cast(Database.StreamVal, value)

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
        if key in self.key_types and self.key_types[key] != Database.ValType.STREAM:
            raise Exception(f"key {key} is not a stream")
        self.key_types[key] = Database.ValType.STREAM
        if key not in self.store:
            self.store[key] = []
        cur_value = cast(Database.StreamVal, self.store[key])
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
        value = cast(Database.StreamVal, value)

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
            return constants.EMPTY_RESP_ARRAY.encode()
        hi = bisect.bisect_right(value, end_stream_id, key=lambda x: x[0])
        if hi >= len(value):
            hi = len(value)

        res = []
        for i in range(lo - 1 if lo != 0 else 0, hi):
            flattened_kvs = [
                RespBulkString(item.encode())
                for items in value[i][1].items()
                for item in items
            ]
            res.append(
                RespArray(
                    [
                        RespBulkString(str(value[i][0]).encode()),
                        RespArray(flattened_kvs),
                    ]
                )
            )
        return RespArray(res).encode()

    def xread(
        self, stream_keys: list[str], ids: list[str], timeout: int | None
    ) -> bytes:
        if timeout is not None:
            original_lens = [len(self.store[stream_key]) for stream_key in stream_keys]
            logger.info(f"{original_lens=}")
            if timeout != 0:
                time.sleep(timeout / 1e3)
            else:
                # wait until there is a new element
                # just need a map of stream_keys to threads to wake up
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
            value = cast(Database.StreamVal, value)
            if id == "$":
                logger.info(f"{original_lens[i]=}")
                id = str(value[original_lens[i] - 1][0]) if value else "0-0"
            stream_id = StreamId(id)

            lo = bisect.bisect_right(value, stream_id, key=lambda x: x[0])
            if lo >= len(value):
                return constants.NULL_BULK_RESP_STRING.encode()

            inter: list[RespDataType] = []
            for i in range(lo, len(value)):
                flattened_kvs = []
                for k, v in value[i][1].items():
                    flattened_kvs.append(RespBulkString(k.encode()))
                    flattened_kvs.append(RespBulkString(v.encode()))
                inter.append(
                    RespArray(
                        [
                            RespBulkString(str(value[i][0]).encode()),
                            RespArray(flattened_kvs),
                        ]
                    )
                )
            res.append(
                RespArray(
                    [
                        RespBulkString(stream_key.encode()),
                        RespArray(inter),
                    ]
                )
            )
        return RespArray(res).encode()


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
