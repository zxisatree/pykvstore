import socket

import database


def construct_conn_id(conn: socket.socket) -> database.Database.ConnId:
    return (conn.fileno(), conn.getsockname())
