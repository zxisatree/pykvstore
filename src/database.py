from threading import RLock
from datetime import datetime


class SingletonMeta(type):
    _instance = None

    def __call__(cls, *args, **kwargs):
        if cls._instance is None:
            instance = super().__call__(*args, **kwargs)
            cls._instance = instance
        return cls._instance


class Database(metaclass=SingletonMeta):
    lock = RLock()
    store: dict[str, tuple[str, datetime | None]] = {}

    def __len__(self) -> int:
        with self.lock:
            return len(self.store)

    def __getitem__(self, key: str) -> str:
        with self.lock:
            if key not in self.store or self.expire_one(key):
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
            return key in map(lambda x: x[0], self.store.keys())

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
