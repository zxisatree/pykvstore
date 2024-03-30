from datetime import datetime, timedelta

import commands
import data_types


# *2\r\n$4\r\necho\r\n$3\r\nhey\r\n = ["echo", "hey"] encoded using the Redis protocol
def parse_cmd(cmd_bytes: bytes) -> commands.Command:
    try:
        cmd = cmd_bytes.decode()
    except:
        # either invalid cmd or RDB file
        return commands.RdbFileCommand(cmd_bytes)
    resp_data, pos = parse(cmd, 0)
    print(f"Codec.parse {resp_data=}, {pos=}")
    if not isinstance(resp_data, data_types.RespArray):
        exception_msg = (
            f"Unsupported command (is not array) {resp_data}, {type(resp_data)}"
        )
        print(f"Raising exception: {exception_msg}")
        raise Exception(exception_msg)

    cmd_resp = resp_data[0]
    if not isinstance(cmd_resp, data_types.RespBulkString):
        exception_msg = f"Unsupported command (first element is not bulk string) {resp_data[0]}, {type(resp_data[0])}"
        print(exception_msg)
        raise Exception(exception_msg)
    cmd_str = cmd_resp.data.upper()
    if cmd_str == "PING":
        return commands.PingCommand()
    elif cmd_str == "ECHO":
        msg = resp_data[1]
        if not isinstance(msg, data_types.RespBulkString):
            exception_msg = f"Unsupported command (second element is not bulk string) {resp_data[1]}, {type(resp_data[1])}"
            print(exception_msg)
            raise Exception(exception_msg)
        return commands.EchoCommand(msg)
    elif cmd_str == "SET":
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
            return commands.SetCommand(cmd, key, value, None)
        # parse px command
        px_cmd = resp_data[3]
        expiry = resp_data[4]
        if not isinstance(px_cmd, data_types.RespBulkString):
            exception_msg = f"Unsupported command (fourth element is not bulk string) {resp_data[3]}, {type(resp_data[3])}"
            print(exception_msg)
            raise Exception(exception_msg)
        if px_cmd.data.upper() != "PX":
            exception_msg = f"Unsupported command (fourth element is not 'PX') {resp_data[3]}, {type(resp_data[3])}"
            print(exception_msg)
            raise Exception(exception_msg)
        if not isinstance(expiry, data_types.RespBulkString):
            exception_msg = f"Unsupported command (fifth element is not bulk string) {resp_data[4]}, {type(resp_data[4])}"
            print(exception_msg)
            raise Exception(exception_msg)
        return commands.SetCommand(
            cmd,
            key,
            value,
            datetime.now() + timedelta(milliseconds=int(expiry.data)),
        )
    elif cmd_str == "GET":
        key = resp_data[1]
        if not isinstance(key, data_types.RespBulkString):
            exception_msg = f"Unsupported command (second element is not bulk string) {resp_data[1]}, {type(resp_data[1])}"
            print(exception_msg)
            raise Exception(exception_msg)
        return commands.GetCommand(key)
    elif cmd_str == "COMMAND":
        return commands.CommandCommand()
    elif cmd_str == "INFO":
        # should check for next word, but only replication is supported
        return commands.InfoCommand()
    elif cmd_str == "REPLCONF":
        return commands.ReplConfCommand()
    elif cmd_str == "PSYNC":
        return commands.PsyncCommand()
    else:
        exception_msg = f"Unsupported command {cmd_str}, {type(cmd_str)}"
        print(exception_msg)
        raise Exception(exception_msg)


def parse(cmd: str, pos: int) -> tuple[data_types.RespDataType, int]:
    # Dispatches parsing to the relevant methods
    data_type = cmd[pos]
    if data_type == "*":
        return data_types.RespArray.decode(cmd, pos + 1)
    elif data_type == "$":
        return data_types.RespBulkString.decode(cmd, pos + 1)
    else:
        print(f"Raising exception: Unsupported data type {data_type}")
        raise Exception(f"Unsupported data type {data_type}")


def is_sep(data: str, pos: int) -> bool:
    return (
        pos + 1 < len(data)
        and data[pos : pos + 1] == "\r"
        and data[pos + 1 : pos + 2] == "\n"
    )
