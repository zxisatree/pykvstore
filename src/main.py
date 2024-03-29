import socket
import threading

import codec


def main():
    try:
        socket.setdefaulttimeout(60)
        server_socket = socket.create_server(("localhost", 6379))
        while True:
            print("main thread waiting...")
            conn, addr = server_socket.accept()
            threading.Thread(target=handle_conn, args=(conn, addr)).start()
    except Exception as e:
        print(f"Exception: {e}")


def handle_conn(conn: socket.socket, addr):
    with conn:
        while True:
            data = conn.recv(1024)
            if not data:
                break
            print(f"raw {data=}")
            cmd = codec.parse_cmd(data)
            conn.sendall(cmd.execute().encode())
        print(f"Connection closed: {addr=}")


if __name__ == "__main__":
    from sys import path
    from pathlib import Path

    # modify path to for test folder structure
    path.append(Path(__file__).parent.as_uri())
    main()
