import argparse
import socket
import threading

from sys import path
from pathlib import Path

# modify path to for test folder structure
path.append(str(Path(__file__).parent))

import codec
import database
import replicas


def main():
    argparser = setup_argpaser()
    args = argparser.parse_args()
    db = database.Database()
    replica_handler = replicas.ReplicaHandler(True)

    try:
        socket.setdefaulttimeout(30)
        server_socket = socket.create_server(("localhost", args.port))
        while True:
            print("main thread waiting...")
            conn, addr = server_socket.accept()
            threading.Thread(
                target=handle_conn, args=(conn, addr, db, replica_handler)
            ).start()
    except Exception as e:
        print(f"Exception: {e}")


def handle_conn(
    conn: socket.socket,
    addr,
    db: database.Database,
    replica_handler: replicas.ReplicaHandler,
):
    with conn:
        while True:
            data = conn.recv(1024)
            if not data:
                break
            print(f"raw {data=}")
            cmd = codec.parse_cmd(data)
            executed = cmd.execute(db, replica_handler)
            print(f"returning {executed=}")
            conn.sendall(executed.encode())
        print(f"Connection closed: {addr=}")


def setup_argpaser() -> argparse.ArgumentParser:
    argparser = argparse.ArgumentParser(
        prog="Readthis", description="Redis clone for Windows"
    )
    argparser.add_argument(
        "--port", type=int, default=6379, help="Port to listen on (default: 6379)"
    )
    return argparser


if __name__ == "__main__":
    main()
