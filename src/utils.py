import socket

ConnId = tuple[int, str]


def construct_conn_id(conn: socket.socket) -> ConnId:
    return (conn.fileno(), conn.getsockname())


def transform_to_execute_output(single_result: str) -> list[bytes]:
    return [single_result.encode()]
