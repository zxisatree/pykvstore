from datetime import datetime
from threading import RLock
import os

import singleton_meta
import rdb


class Database(metaclass=singleton_meta.SingletonMeta):
    lock = RLock()
    store: dict[str, tuple[str, datetime | None]] = {}

    def __init__(self, dir: str, dbfilename: str):
        self.dir = dir
        self.dbfilename = dbfilename
        file_path = os.path.join(self.dir, self.dbfilename)
        print(f"{os.path.exists(file_path)=}")
        if os.path.exists(file_path):
            with open(file_path, "rb") as f:
                self.rdb = rdb.RdbFile(f.read())
        else:
            self.rdb = rdb.RdbFile(b"")

    def __len__(self) -> int:
        with self.lock:
            return len(self.store)

    def __getitem__(self, key: str) -> str:
        with self.lock:
            if key not in self.store:
                return ""
            if self.store[key][1] and self.expire_one(key):
                return ""
            return self.store[key][0]

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

    def expire(self):
        with self.lock:
            for key, (_, expiry) in self.store.items():
                if expiry and expiry < datetime.now():
                    del self.store[key]

    def expire_one(self, key: str) -> bool:
        # returns True if key was expired
        with self.lock:
            expiry = self.store[key][1]
            if expiry and expiry < datetime.now():
                del self.store[key]
                return True
            return False
