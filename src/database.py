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
        self.rdb = rdb.RdbFile(
            b"REDIS0003\xfa\tredis-ver\x057.2.0\xfa\nredis-bits\xc0@\xfe\x00\xfb\x04\x04\xfc\x00\x9c\xef\x12~\x01\x00\x00\x00\x04pear\x06orange\xfc\x00\x0c(\x8a\xc7\x01\x00\x00\x00\x05grape\nstrawberry\xfc\x00\x0c(\x8a\xc7\x01\x00\x00\x00\x05mango\x04pear\xfc\x00\x0c(\x8a\xc7\x01\x00\x00\x00\x06orange\x05apple\xff\x93/\xd7\xf5\xe6\xe2\x14\xcf\n"
        )
        # if os.path.exists(file_path):
        #     with open(file_path, "rb") as f:
        #         self.rdb = rdb.RdbFile(f.read())
        # else:
        #     self.rdb = rdb.RdbFile(b"")
        for key, value in self.rdb.key_values.items():
            self.store[key] = value
        print(f"db initialised with {self.store=}")

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
            print(f"{expiry=}, {datetime.now()=}")
            if expiry and expiry < datetime.now():
                del self.store[key]
                return True
            return False
