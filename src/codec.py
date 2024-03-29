import commands
import data_types


# *2\r\n$4\r\necho\r\n$3\r\nhey\r\n = ["echo", "hey"] encoded using the Redis protocol
def parse_cmd(cmd_bytes: bytes) -> commands.Command:
    cmd = cmd_bytes.decode()
    resp_data, pos = parse(cmd, 0)
    print(f"Codec.parse {resp_data=}, {pos=}")
    if not isinstance(resp_data, data_types.RespArray):
        print(
            f"Raising exception: Unsupported command (is not array) {resp_data}, {type(resp_data)}"
        )
        raise Exception(
            f"Unsupported command (is not array) {resp_data}, {type(resp_data)}"
        )

    cmd_resp = resp_data[0]
    if not isinstance(cmd_resp, data_types.RespBulkString):
        print(
            f"Raising exception: Unsupported command (first element is not bulk string) {resp_data[0]}, {type(resp_data[0])}"
        )
        raise Exception(
            f"Unsupported command (first element is not bulk string) {resp_data[0]}, {type(resp_data[0])}"
        )
    cmd_str = cmd_resp.data.upper()
    if cmd_str == "PING":
        return commands.PingCommand()
    elif cmd_str == "ECHO":
        msg = resp_data[1]
        if not isinstance(msg, data_types.RespBulkString):
            print(
                f"Raising exception: Unsupported command (second element is not bulk string) {resp_data[1]}, {type(resp_data[1])}"
            )
            raise Exception(
                f"Unsupported command (second element is not bulk string) {resp_data[1]}, {type(resp_data[1])}"
            )
        return commands.EchoCommand(msg)
    elif cmd_str == "COMMAND":
        return commands.CommandCommand()
    elif cmd_str == "INFO":
        return commands.InfoCommand()
    else:
        print(f"Raising exception: Unsupported command {cmd_str}, {type(cmd_str)}")
        raise Exception(f"Unsupported command {cmd_str}, {type(cmd_str)}")


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
