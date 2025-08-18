from collections import defaultdict
import socket
from threading import Lock, RLock

ConnId = tuple[int, str]


def construct_conn_id(conn: socket.socket) -> ConnId:
    return (conn.fileno(), conn.getsockname())


def transform_to_execute_output(single_result: str) -> list[bytes]:
    return [single_result.encode()]


class ThreadsafeDict[KT, VT](dict):
    """Coarse locking wrapper over a dict"""

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
    """Coarse locking wrapper over a defaultdict"""

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
