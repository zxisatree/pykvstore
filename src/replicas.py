import threading
import uuid
import socket

import singleton_meta
import codec
import commands
import constants
import database
import data_types


class ReplicaHandler(metaclass=singleton_meta.SingletonMeta):
    def __init__(
        self,
        is_master: bool,
        ip: str,
        port: int,
        replica_of: list,
        db: database.Database,
    ):
        self.is_master = is_master
        self.id = str(uuid.uuid4())
        self.ip = ip
        self.port = port
        if replica_of:
            self.master_ip = replica_of[0]
            self.master_port = replica_of[1]
        self.slaves: list[socket.socket] = []
        self.info = {
            "role": "master" if is_master else "slave",
            "connected_slaves": len(self.slaves),
            "master_replid": self.id if is_master else "?",
            "master_repl_offset": 0 if is_master else -1,
            # "second_repl_offset": -1,
            # "repl_backlog_active": 0,
            # "repl_backlog_size": 1048576,
            # "repl_backlog_first_byte_offset": 0,
            # "repl_backlog_histlen": 0,
        }
        # attempt to connect to master
        if not is_master:
            threading.Thread(target=self.connect_to_master, args=(db,)).start()

    def connect_to_master(self, db: database.Database):
        self.master_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.master_conn.settimeout(constants.CONN_TIMEOUT)
        self.master_conn.connect((self.master_ip, int(self.master_port)))
        self.master_conn.sendall(
            data_types.RespArray([data_types.RespBulkString("ping")]).encode().encode()
        )
        data = self.master_conn.recv(constants.BUFFER_SIZE)
        print(f"Replica sent ping, got {data=}")
        # check if we get PONG
        if data.decode() != commands.PingCommand().execute(None, None, None):
            print("Failed to connect to master")
        self.master_conn.sendall(
            data_types.RespArray(
                [
                    data_types.RespBulkString("REPLCONF"),
                    data_types.RespBulkString("listening-port"),
                    data_types.RespBulkString(str(self.port)),
                ]
            )
            .encode()
            .encode()
        )
        data = self.master_conn.recv(constants.BUFFER_SIZE)
        print(f"Replica sent REPLCONF 1, got {data=}")
        # check if we get OK
        if data.decode() != constants.OK_SIMPLE_STRING:
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
        data = self.master_conn.recv(constants.BUFFER_SIZE)
        print(f"Replica sent REPLCONF 2, got {data=}")
        # check if we get OK
        if data.decode() != constants.OK_SIMPLE_STRING:
            print("Failed to connect to master")
        self.master_conn.sendall(
            data_types.RespArray(
                [
                    data_types.RespBulkString("PSYNC"),
                    data_types.RespBulkString(str(self.info["master_replid"])),
                    data_types.RespBulkString(str(self.info["master_repl_offset"])),
                ]
            )
            .encode()
            .encode()
        )
        print(f"Replica sent PSYNC")

        while True:
            print("Replica waiting for master...")
            data = self.master_conn.recv(constants.BUFFER_SIZE)
            print(f"from master: raw {data=}")
            if not data:
                print("Replica breaking")
                break
            cmds = codec.parse_cmd(data)
            if isinstance(cmds, list):
                for cmd in cmds:
                    self.respond_to_master(cmd, db)
            else:
                self.respond_to_master(cmds, db)

    def respond_to_master(self, cmd, db):
        executed = cmd.execute(db, self, self.master_conn)
        if isinstance(cmd, commands.ReplConfGetAckCommand):
            if isinstance(executed, str):  # impossible to get list here
                print(f"responding {executed}")
                self.master_conn.sendall(executed.encode())

    def propogate(self, raw_cmd: str):
        for slave in self.slaves:
            print(f"Propogating to {slave=}")
            slave.sendall(raw_cmd.encode())

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
