from datetime import datetime
from threading import RLock
import os

import singleton_meta
import rdb


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

    def validate_stream_id(self, key: str, id: str) -> bool:
        with self.lock:
            if key not in self.store:
                return True
            cur_value = self.store[key]
            if not isinstance(cur_value, list):
                return False
            milliseconds_time, seq_no = id.split("-")
            if int(milliseconds_time) < 0 or (
                int(milliseconds_time) < 0 and int(seq_no) < 1
            ):
                return False
            if not cur_value:
                return True
            print(f"{cur_value[-1]=}")
            last_mst, last_seq_no = cur_value[-1]["id"].split("-")
            if int(milliseconds_time) < int(last_mst):
                return False
            elif int(milliseconds_time) == int(last_mst) and int(seq_no) <= int(
                last_seq_no
            ):
                return False
            return True

    def xadd(self, key: str, id: str, value: dict):
        with self.lock:
            if key not in self.store:
                self.store[key] = []
            cur_value = self.store[key]
            if not isinstance(cur_value, list):
                print(f"stream key {key} is not a stream")
                raise Exception(f"stream key {key} is not a stream")
            cur_value.append({"id": id, **value})
