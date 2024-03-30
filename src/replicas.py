import uuid
import socket

import singleton_meta
import commands
import data_types


class ReplicaHandler(metaclass=singleton_meta.SingletonMeta):
    def __init__(self, is_master: bool, ip: str, port: int, replica_of: list):
        self.is_master = is_master
        self.id = str(uuid.uuid4())
        self.master_ip = replica_of[0]
        self.master_port = replica_of[1]
        self.ip = ip
        self.port = port
        self.info = {
            "role": "master" if is_master else "slave",
            "connected_slaves": 0,
            "master_replid": self.id,
            "master_repl_offset": 0,
            "second_repl_offset": -1,
            "repl_backlog_active": 0,
            "repl_backlog_size": 1048576,
            "repl_backlog_first_byte_offset": 0,
            "repl_backlog_histlen": 0,
        }
        # attempt to connect to master
        if not is_master:
            self.master_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.master_conn.settimeout(10)
            self.master_conn.connect((replica_of[0], int(replica_of[1])))
            self.master_conn.sendall(
                commands.PingCommand().execute(None, None).encode()
            )
            self.master_conn.sendall(
                data_types.RespArray(
                    [
                        data_types.RespBulkString("REPLCONF"),
                        data_types.RespBulkString("listening-port"),
                        data_types.RespBulkString(str(port)),
                    ]
                )
                .encode()
                .encode()
            )
            self.master_conn.sendall(
                data_types.RespArray(
                    [
                        data_types.RespBulkString("REPLCONF"),
                        data_types.RespBulkString("capa"),
                        data_types.RespBulkString("psync2"),
                    ]
                )
                .encode()
                .encode()
            )

    def get_info(self) -> str:
        # encode each kv as a RespBulkString
        return data_types.RespBulkString(
            "".join(
                map(
                    lambda item: data_types.RespBulkString(
                        f"{item[0]}:{item[1]}"
                    ).encode(),
                    self.info.items(),
                )
            )
        ).encode()
