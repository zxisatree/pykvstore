import uuid
import socket

import singleton_meta
import commands
import constants
import data_types


class ReplicaHandler(metaclass=singleton_meta.SingletonMeta):
    def __init__(self, is_master: bool, ip: str, port: int, replica_of: list):
        self.is_master = is_master
        self.id = str(uuid.uuid4())
        self.ip = ip
        self.port = port
        if replica_of:
            self.master_ip = replica_of[0]
            self.master_port = replica_of[1]
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
                data_types.RespArray([data_types.RespBulkString("ping")])
                .encode()
                .encode()
            )
            data = self.master_conn.recv(1024)
            print(f"Replica sent ping, got data {data=}")
            # check if we get PONG
            if data.decode() != commands.PingCommand().execute(None, None):
                print("Failed to connect to master")
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
            data = self.master_conn.recv(1024)
            print(f"Replica sent REPLCONF 1, got data {data=}")
            # check if we get OK
            if data.decode() != constants.OK_RESPONSE:
                print("Failed to connect to master")
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
            data = self.master_conn.recv(1024)
            print(f"Replica sent REPLCONF 2, got data {data=}")
            # check if we get OK
            if data.decode() != constants.OK_RESPONSE:
                print("Failed to connect to master")

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
