import socket

ConnId = tuple[int, str]


def construct_conn_id(conn: socket.socket) -> ConnId:
    return (conn.fileno(), conn.getsockname())
