from datetime import datetime, timedelta

import commands
import data_types


# *2\r\n$4\r\necho\r\n$3\r\nhey\r\n = ["echo", "hey"] encoded using the Redis protocol
def parse_cmd(cmd: bytes) -> commands.Command | list[commands.Command]:
    final_cmds: list[commands.Command] = []
    pos = 0
    while pos < len(cmd):
        orig = pos
        resp_data, pos = parse(cmd, pos)
        print(f"Codec.parse {resp_data=}, {pos=}")
        if isinstance(resp_data, data_types.RespArray):
            final_cmds.append(parse_resp_cmd(resp_data, cmd, orig, pos))
        elif isinstance(resp_data, data_types.RespSimpleString):
            # is +FULLRESYNC
            final_cmds.append(commands.FullResyncCommand(resp_data.data))
        elif isinstance(resp_data, data_types.RdbFile):
            final_cmds.append(commands.RdbFileCommand(resp_data.data))
        else:
            exception_msg = (
                f"Unsupported command (is not array) {resp_data}, {type(resp_data)}"
            )
            print(f"Raising exception: {exception_msg}")
            raise Exception(exception_msg)
    return final_cmds


def parse_resp_cmd(
    resp_data: data_types.RespArray, cmd: bytes, start: int, end: int
) -> commands.Command:
    cmd_resp = resp_data[0]
    if not isinstance(cmd_resp, data_types.RespBulkString):
        exception_msg = f"Unsupported command (first element is not bulk string) {resp_data[0]}, {type(resp_data[0])}"
        print(exception_msg)
        raise Exception(exception_msg)
    cmd_str = cmd_resp.data.upper()
    if cmd_str == b"PING":
        return commands.PingCommand(cmd[start:end])
    elif cmd_str == b"ECHO":
        msg = resp_data[1]
        if not isinstance(msg, data_types.RespBulkString):
            exception_msg = f"Unsupported command (second element is not bulk string) {resp_data[1]}, {type(resp_data[1])}"
            print(exception_msg)
            raise Exception(exception_msg)
        return commands.EchoCommand(cmd[start:end], msg)
    elif cmd_str == b"SET":
        key = resp_data[1]
        value = resp_data[2]
        if not isinstance(key, data_types.RespBulkString):
            exception_msg = f"Unsupported command (second element is not bulk string) {resp_data[1]}, {type(resp_data[1])}"
            print(exception_msg)
            raise Exception(exception_msg)
        if not isinstance(value, data_types.RespBulkString):
            exception_msg = f"Unsupported command (third element is not bulk string) {resp_data[2]}, {type(resp_data[2])}"
            print(exception_msg)
            raise Exception(exception_msg)
        if len(resp_data) <= 3:
            return commands.SetCommand(cmd[start:end], key, value, None)
        # parse px command
        px_cmd = resp_data[3]
        expiry = resp_data[4]
        if not isinstance(px_cmd, data_types.RespBulkString):
            exception_msg = f"Unsupported command (fourth element is not bulk string) {resp_data[3]}, {type(resp_data[3])}"
            print(exception_msg)
            raise Exception(exception_msg)
        if px_cmd.data.upper() != b"PX":
            exception_msg = f"Unsupported command (fourth element is not 'PX') {resp_data[3]}, {type(resp_data[3])}"
            print(exception_msg)
            raise Exception(exception_msg)
        if not isinstance(expiry, data_types.RespBulkString):
            exception_msg = f"Unsupported command (fifth element is not bulk string) {resp_data[4]}, {type(resp_data[4])}"
            print(exception_msg)
            raise Exception(exception_msg)
        return commands.SetCommand(
            cmd[start:end],
            key,
            value,
            datetime.now() + timedelta(milliseconds=int(expiry.data)),
        )
    elif cmd_str == b"GET":
        key = resp_data[1]
        if not isinstance(key, data_types.RespBulkString):
            exception_msg = f"Unsupported command (second element is not bulk string) {resp_data[1]}, {type(resp_data[1])}"
            print(exception_msg)
            raise Exception(exception_msg)
        return commands.GetCommand(cmd[start:end], key)
    elif cmd_str == b"COMMAND":
        return commands.CommandCommand(cmd[start:end])
    elif cmd_str == b"INFO":
        # should check for next word, but only replication is supported
        return commands.InfoCommand(cmd[start:end])
    elif cmd_str == b"REPLCONF":
        if len(resp_data) >= 3:
            cmd_str2 = resp_data[1]
            if not isinstance(cmd_str2, data_types.RespBulkString):
                exception_msg = f"Unsupported command (second element is not bulk string) {resp_data[1]}, {type(resp_data[1])}"
                print(exception_msg)
                raise Exception(exception_msg)
            if cmd_str2.data.upper() == b"GETACK":
                print(f"parse_cmd got ReplConfGetAckCommand")
                return commands.ReplConfGetAckCommand(cmd[start:end])
            elif cmd_str2.data.upper() == b"ACK":
                print(f"parse_cmd got ReplConfAckCommand")
                return commands.ReplConfAckCommand(cmd[start:end])
            return commands.ReplConfCommand(cmd[start:end])
        print(f"parse_cmd got ReplConfCommand")
        return commands.ReplConfCommand(cmd[start:end])
    elif cmd_str == b"WAIT":
        replica_count = resp_data[1]
        timeout = resp_data[2]
        if not isinstance(replica_count, data_types.RespBulkString):
            exception_msg = f"Unsupported command (second element is not bulk string) {resp_data[1]}, {type(resp_data[1])}"
            print(exception_msg)
            raise Exception(exception_msg)
        if not isinstance(timeout, data_types.RespBulkString):
            exception_msg = f"Unsupported command (third element is not bulk string) {resp_data[2]}, {type(resp_data[2])}"
            print(exception_msg)
            raise Exception(exception_msg)
        print(f"returning WaitCommand")
        return commands.WaitCommand(
            cmd[start:end], int(replica_count.data), int(timeout.data)
        )
    elif cmd_str == b"PSYNC":
        return commands.PsyncCommand(cmd[start:end])
    elif cmd_str == b"CONFIG":
        key = resp_data[1]
        if not isinstance(key, data_types.RespBulkString):
            exception_msg = f"Unsupported command (second element is not bulk string) {resp_data[1]}, {type(resp_data[1])}"
            print(exception_msg)
            raise Exception(exception_msg)
        return commands.ConfigGetCommand(cmd[start:end], key.data)
    else:
        return commands.RdbFileCommand(cmd[start:end])
        # exception_msg = f"Unsupported command {cmd_str}, {type(cmd_str)}"
        # print(exception_msg)
        # raise Exception(exception_msg)


def parse(cmd: bytes, pos: int) -> tuple[data_types.RespDataType, int]:
    # Dispatches parsing to the relevant methods
    data_type = cmd[pos : pos + 1]
    if data_type == b"*":
        return data_types.RespArray.decode(cmd, pos)
    elif data_type == b"$":
        return data_types.decode_bulk_string_or_rdb(cmd, pos)
    elif data_type == b"+":
        return data_types.RespSimpleString.decode(cmd, pos)
    else:
        print(f"Raising exception: Unsupported data type {data_type}")
        raise Exception(f"Unsupported data type {data_type}")


def is_sep(data: bytes, pos: int) -> bool:
    # using slices to index data to get bytes instead of ints
    return (
        pos + 1 < len(data)
        and data[pos : pos + 1] == b"\r"
        and data[pos + 1 : pos + 2] == b"\n"
    )
