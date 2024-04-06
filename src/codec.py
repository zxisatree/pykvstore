from datetime import datetime, timedelta

import commands
import data_types
from logs import logger


def parse_cmd(cmd: bytes) -> list[commands.Command]:
    final_cmds: list[commands.Command] = []
    pos = 0
    while pos < len(cmd):
        orig = pos
        resp_data, pos = dispatch(cmd, pos)
        logger.info(f"Codec.parse {resp_data=}, {pos=}")
        if isinstance(resp_data, data_types.RespArray):
            final_cmds.append(parse_resp_cmd(resp_data, cmd, orig, pos))
        elif isinstance(resp_data, data_types.RespSimpleString):
            # is +FULLRESYNC
            final_cmds.append(commands.FullResyncCommand(resp_data.data))
        elif isinstance(resp_data, data_types.RespRdbFile):
            final_cmds.append(commands.RdbFileCommand(resp_data.data.data))
        else:
            logger.error(
                f"Unsupported command (is not array) {resp_data}, {type(resp_data)}"
            )
            final_cmds.append(commands.NoOp(cmd[orig:pos]))
    return final_cmds


def parse_resp_cmd(
    resp_data: data_types.RespArray, cmd: bytes, start: int, end: int
) -> commands.Command:
    try:
        cmd_str = data_types.RespBulkString.validate(resp_data[0]).data.upper()
        raw_cmd = cmd[start:end]
        if cmd_str == b"PING":
            return commands.PingCommand(raw_cmd)
        elif cmd_str == b"ECHO":
            msg = data_types.RespBulkString.validate(resp_data[1])
            return commands.EchoCommand(raw_cmd, msg)
        elif cmd_str == b"SET":
            key = data_types.RespBulkString.validate(resp_data[1])
            value = data_types.RespBulkString.validate(resp_data[2])
            if len(resp_data) <= 3:
                return commands.SetCommand(raw_cmd, key, value, None)
            px_cmd = data_types.RespBulkString.validate(resp_data[3])
            expiry = data_types.RespBulkString.validate(resp_data[4])
            commands.SetCommand.validate_px(px_cmd)
            return commands.SetCommand(
                raw_cmd,
                key,
                value,
                datetime.now() + timedelta(milliseconds=int(expiry.data)),
            )
        elif cmd_str == b"GET":
            key = data_types.RespBulkString.validate(resp_data[1])
            return commands.GetCommand(raw_cmd, key)
        elif cmd_str == b"COMMAND":
            return commands.CommandCommand(raw_cmd)
        elif cmd_str == b"INFO":
            # should check for next word, but only replication is supported
            return commands.InfoCommand(raw_cmd)
        elif cmd_str == b"REPLCONF":
            if len(resp_data) >= 3:
                cmd_str2 = data_types.RespBulkString.validate(resp_data[1])
                if cmd_str2.data.upper() == b"GETACK":
                    return commands.ReplConfGetAckCommand(raw_cmd)
                elif cmd_str2.data.upper() == b"ACK":
                    return commands.ReplConfAckCommand(raw_cmd)
            return commands.ReplConfCommand(raw_cmd)
        elif cmd_str == b"WAIT":
            replica_count = data_types.RespBulkString.validate(resp_data[1])
            timeout = data_types.RespBulkString.validate(resp_data[2])
            return commands.WaitCommand(
                raw_cmd, int(replica_count.data), int(timeout.data)
            )
        elif cmd_str == b"PSYNC":
            return commands.PsyncCommand(raw_cmd)
        elif cmd_str == b"CONFIG":
            key = data_types.RespBulkString.validate(resp_data[2])
            return commands.ConfigGetCommand(raw_cmd, key.data)
        elif cmd_str == b"KEYS":
            pattern = data_types.RespBulkString.validate(resp_data[1])
            return commands.KeysCommand(raw_cmd, pattern.data)
        elif cmd_str == b"TYPE":
            key = data_types.RespBulkString.validate(resp_data[1])
            return commands.TypeCommand(raw_cmd, key.data)
        elif cmd_str == b"XADD":
            stream_key = data_types.RespBulkString.validate(resp_data[1])
            return commands.XaddCommand(
                raw_cmd, stream_key.data, resp_data.elements[2:]
            )
        elif cmd_str == b"XRANGE":
            key = data_types.RespBulkString.validate(resp_data[1])
            xrange_start = data_types.RespBulkString.validate(resp_data[2])
            xrange_end = data_types.RespBulkString.validate(resp_data[3])
            return commands.XrangeCommand(
                raw_cmd,
                key.data,
                xrange_start.data.decode(),
                xrange_end.data.decode(),
            )
        elif cmd_str == b"XREAD":
            # first argument should be "streams"
            streams = data_types.RespBulkString.validate(resp_data[1])
            key_id_start_idx = 2
            is_block = False
            if streams.data.upper() == b"BLOCK":
                is_block = True
                key_id_start_idx = 4
            remaining_len = len(resp_data) - key_id_start_idx
            # TODO: fix type checking
            keys = list(map(lambda x: x.data.decode(), resp_data.elements[key_id_start_idx : key_id_start_idx + remaining_len // 2]))  # type: ignore
            ids = list(map(lambda x: x.data.decode(), resp_data.elements[key_id_start_idx + remaining_len // 2 :]))  # type: ignore
            if is_block:
                return commands.XreadCommand(
                    raw_cmd, keys, ids, int(resp_data[2].data.decode())  # type: ignore
                )
            else:
                return commands.XreadCommand(raw_cmd, keys, ids)
        else:
            return commands.RdbFileCommand(raw_cmd)
    except Exception:
        logger.error(f"Unsupported command {cmd_str}, {type(cmd_str)}")
        return commands.NoOp(raw_cmd)


def dispatch(cmd: bytes, pos: int) -> tuple[data_types.RespDataType, int]:
    data_type = cmd[pos : pos + 1]
    if data_type == b"*":
        return data_types.RespArray.decode(cmd, pos)
    elif data_type == b"$":
        return data_types.decode_bulk_string_or_rdb(cmd, pos)
    elif data_type == b"+":
        return data_types.RespSimpleString.decode(cmd, pos)
    else:
        logger.info(f"Raising exception: Unsupported data type {data_type}")
        raise Exception(f"Unsupported data type {data_type}")
