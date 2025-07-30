import argparse
import os
import select
import socket
import threading

from sys import path
from pathlib import Path

# modify path to call files in parent folder
path.append(str(Path(__file__).parent))

import codec
import commands
import constants
import database
import data_types
from logs import logger
from utils import construct_conn_id
import replicas


def main():
    either = validate_parse_args(setup_argparser().parse_args())
    if either[1] is not None:
        logger.error(f"Error parsing command line arguments: {either[1]}")
        return
    port, replicaof, rdbdir, dbfilename = either[0]
    db = database.Database(rdbdir, dbfilename)
    replica_handler = replicas.ReplicaHandler(
        False if replicaof else True, "localhost", port, replicaof, db
    )

    try:
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        socket.create_server
        if os.name not in ("nt", "cygwin"):
            try:
                server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            except:
                # dirty fix for non linux to just ignore the error
                pass
        server_socket.bind(("localhost", port))
        logger.info(f"Started server on {port=}")
        while True:
            server_socket.listen()
            ready = select.select([server_socket], [], [], 0.5)
            if ready[0]:
                conn, addr = server_socket.accept()
                thread = threading.Thread(
                    target=handle_conn, args=(conn, addr, db, replica_handler)
                )
                thread.daemon = True
                thread.start()
    except Exception:
        logger.exception("main thread exception")


def handle_conn(
    conn: socket.socket,
    addr,
    db: database.Database,
    replica_handler: replicas.ReplicaHandler,
):
    with conn:
        while True:
            data = conn.recv(constants.BUFFER_SIZE)
            if not data:
                break
            logger.info(f"raw {data=}")
            cmds = codec.parse_cmd(data)
            for cmd in cmds:
                execute_cmd(cmd, db, replica_handler, conn)

        logger.info(f"Connection closed: {addr=}")


def execute_cmd(
    cmd: commands.Command,
    db: database.Database,
    replica_handler: replicas.ReplicaHandler,
    conn: socket.socket,
):
    conn_id = construct_conn_id(conn)  # TODO: check if this is really static
    if (
        db.xact_exists(conn_id)
        and not isinstance(cmd, commands.ExecCommand)
        and not isinstance(cmd, commands.DiscardCommand)
    ):
        db.queue_xact_cmd(conn_id, cmd)
        executed = constants.XACT_QUEUED_RESPONSE.encode()
    elif (
        db.in_subscribed_mode(conn_id)
        and not isinstance(cmd, commands.SubscribeCommand)
        and not isinstance(cmd, commands.PingCommand)
    ):
        executed = data_types.RespSimpleError(
            f"ERR Can't execute '{cmd.keyword.decode().lower()}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in subscribed mode".encode()
        ).encode()
    elif db.in_subscribed_mode(conn_id) and isinstance(cmd, commands.PingCommand):
        # TODO: should actually be in Commands interface
        cmd.in_subscribed_mode = True
        logger.info(f"cmd.in_subscribed_mode = True")
        executed = cmd.execute(db, replica_handler, conn)
    else:
        executed = cmd.execute(db, replica_handler, conn)

    if isinstance(executed, list):
        for resp in executed:
            logger.info(f"responding {resp}")
            conn.sendall(resp)
    else:
        logger.info(f"responding {executed}")
        conn.sendall(executed)


def validate_parse_args(
    args: argparse.Namespace,
) -> tuple[tuple[int, tuple[str, int] | None, str, str], None] | tuple[None, str]:
    if not isinstance(args.port, int) or args.port < 0 or args.port > 65535:
        return None, "Invalid port number"
    if args.replicaof is not None:
        replica_host, replica_port = args.replicaof.split(" ")
        try:
            replicaof_int = int(replica_port)
        except:
            return None, "replicaof port is not an integer"
        return (
            args.port,
            (replica_host, replicaof_int),
            args.dir,
            args.dbfilename,
        ), None
    return (args.port, None, args.dir, args.dbfilename), None


def setup_argparser() -> argparse.ArgumentParser:
    argparser = argparse.ArgumentParser(
        prog="Readthis", description="Redis clone for Windows"
    )
    argparser.add_argument(
        "--port", type=int, default=6379, help="Port to listen on (default: 6379)"
    )
    argparser.add_argument(
        "--replicaof",
        type=str,
        # nargs=2, # recently changed to a single string e.g. "localhost 6380"
        default=None,
        help="Master IP and port to replicate from (default: None)",
    )
    argparser.add_argument(
        "--dir",
        type=str,
        default="./rdb",
        help="Directory where RDB files are stored (default: ./rdb)",
    )
    argparser.add_argument(
        "--dbfilename",
        type=str,
        default="dump.rdb",
        help="The name of the RDB file (default: dump.rdb)",
    )
    return argparser


if __name__ == "__main__":
    main()
