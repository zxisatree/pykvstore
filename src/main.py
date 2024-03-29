import socket
import threading


def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        conn, addr = server_socket.accept()
        threading.Thread(target=handle_conn, args=(conn, addr)).start()


def handle_conn(conn: socket, addr: socket._RetAddress):
    with conn:
        while True:
            data = conn.recv(1024)
            if not data:
                break
            print(f"{data=}")
            conn.sendall(b"+PONG\r\n")


if __name__ == "__main__":
    main()
