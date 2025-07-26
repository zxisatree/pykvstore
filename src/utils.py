import socket

import database


def construct_conn_id(conn: socket.socket) -> database.Database.ConnIdType:
    return (conn.fileno(), conn.getsockname())
