from datetime import datetime
from threading import RLock
import os

import constants
import rdb
import singleton_meta


class Database(metaclass=singleton_meta.SingletonMeta):
    lock = RLock()
    store: dict[str, tuple[str, datetime | None] | list[dict]] = {}

    def __init__(self, dir: str, dbfilename: str):
        self.dir = dir
        self.dbfilename = dbfilename
        file_path = os.path.join(self.dir, self.dbfilename)
        if os.path.exists(file_path):
            with open(file_path, "rb") as f:
                self.rdb = rdb.RdbFile(f.read())
        else:
            self.rdb = rdb.RdbFile(b"")
        for key, value in self.rdb.key_values.items():
            self.store[key] = value
        print(f"db initialised with {self.store=}")

    def __len__(self) -> int:
        with self.lock:
            return len(self.store)

    def __getitem__(self, key: str) -> str | list[dict] | None:
        with self.lock:
            if key not in self.store:
                return None
            value = self.store[key]
            if isinstance(value, list):
                return value
            if value[1] and self.expire_one(key):
                return None
            return value[0]

    def __setitem__(self, key: str, value: tuple[str, datetime | None]):
        with self.lock:
            self.store[key] = value

    def __delitem__(self, key: str):
        with self.lock:
            del self.store[key]

    def __contains__(self, key: str) -> bool:
        with self.lock:
            return key in map(lambda x: x, self.store.keys())

    def __str__(self) -> str:
        with self.lock:
            return str(self.store)

    def __repr__(self) -> str:
        with self.lock:
            return f"Store({repr(self.store)})"

    def get_type(self, key: str) -> str:
        with self.lock:
            if key not in self.store:
                return "none"
            value = self.store[key]
            if isinstance(value, list):
                return "stream"
            return "string"

    def expire(self):
        with self.lock:
            for key, value in self.store.items():
                if not isinstance(value, list):
                    _, expiry = value
                    if expiry and expiry < datetime.now():
                        del self.store[key]

    def expire_one(self, key: str) -> bool:
        # returns True if key was expired
        with self.lock:
            value = self.store[key]
            if isinstance(value, list):
                return False
            expiry = value[1]
            print(f"{expiry=}, {datetime.now()=}")
            if expiry and expiry < datetime.now():
                del self.store[key]
                return True
            return False

    def validate_stream_id(self, key: str, id: str) -> bytes | None:
        with self.lock:
            if key not in self.store:
                return None
            cur_value = self.store[key]
            if not isinstance(cur_value, list):
                return constants.STREAM_ID_NOT_GREATER_ERROR.encode()
            milliseconds_time, seq_no = id.split("-")
            if int(milliseconds_time) <= 0 and int(seq_no) <= 0:
                return constants.STREAM_ID_TOO_SMALL_ERROR.encode()
            if not cur_value:
                return None
            print(f"{cur_value[-1]=}")
            last_mst, last_seq_no = cur_value[-1]["id"].split("-")
            if int(milliseconds_time) < int(last_mst):
                return constants.STREAM_ID_NOT_GREATER_ERROR.encode()
            elif int(milliseconds_time) == int(last_mst) and int(seq_no) <= int(
                last_seq_no
            ):
                return constants.STREAM_ID_NOT_GREATER_ERROR.encode()
            return None

    def xadd(self, key: str, id: str, value: dict) -> str:
        # stream key has already been validated
        with self.lock:
            if key not in self.store:
                self.store[key] = []
            cur_value = self.store[key]
            if not isinstance(cur_value, list):
                print(f"stream key {key} is not a stream")
                raise Exception(f"stream key {key} is not a stream")
            processed_id = self.process_stream_id(
                id, cur_value[-1]["id"] if cur_value else None
            )
            print(f"got {processed_id=}")
            cur_value.append({"id": processed_id, **value})
            return processed_id

    def process_stream_id(self, id: str, last_id: str | None) -> str:
        splitted = id.split("-")
        if len(splitted) != 2:
            raise Exception(f"Invalid stream id {id}")
        milliseconds_time, seq_no = splitted
        if not last_id:
            if seq_no == "*":
                seq_no = "1" if milliseconds_time == "0" else "0"
            return f"{milliseconds_time}-{seq_no}"

        splitted_last = last_id.split("-")
        last_milliseconds_time, last_seq_no = splitted_last
        print(f"{milliseconds_time=}, {last_milliseconds_time=}")
        print(f"{seq_no=}, {last_seq_no=}")
        if seq_no == "*":
            if milliseconds_time == last_milliseconds_time:
                seq_no = str(int(last_seq_no) + 1)
            else:
                seq_no = "1" if milliseconds_time == "0" else "0"
        return f"{milliseconds_time}-{seq_no}"
