class RdbFile:
    def __init__(self, data: bytes):
        self.data = data
        self.idx = 9  # ignore magic string and version number

    def read_rdb(self):
        sanity_check = self.data[0:5]
        if sanity_check != b"REDIS":
            raise Exception("Invalid RDB file")
        # version_number = int.from_bytes(data[5:9], byteorder="little")
        while self.idx <= len(self.data):
            self.parse()

    def read(self, length: int) -> bytes:
        data = self.data[self.idx : self.idx + length]
        self.idx += length
        return data

    def read_length_encoding(self) -> tuple[int, int, int]:
        length_encoding = self.read(1)
        return (
            int(length_encoding) >> 7,
            (int(length_encoding) >> 6) & 1,
            int(length_encoding) & 0x3F,
        )

    def read_length_encoded_integer(self) -> int:
        le0, le1, rest = self.read_length_encoding()
        if le0 == 0 and le1 == 0:
            return rest
        elif le0 == 0 and le1 == 1:
            next_byte = self.read(1)
            return (rest << 8) | int(next_byte)
        elif le0 == 1 and le0 == 0:
            return int(self.read(4))
        else:
            # TODO
            return 0

    def parse(self) -> list:
        buffer = []
        op_code = self.read(1)
        while self.idx < len(self.data):
            match op_code:
                case b"\xff":
                    break  # EOF
                case b"\xfe":
                    # database selector
                    db_selector = self.read_length_encoded_integer()
                    print(f"got db selector {db_selector=}")
                case b"\xfd":
                    # expiry time in s
                    expiry = self.read(4)
                    print(f"got expiry {expiry=}")
                case b"\xfc":
                    # expiry time in ms
                    expiry = self.read(8)
                    print(f"got expiry {expiry=}")
                case b"\xfb":
                    # resizedb
                    db_hash_table_size = self.read_length_encoded_integer()
                    expiry_hash_table_size = self.read_length_encoded_integer()
                    print(
                        f"got resizedb {db_hash_table_size=}, {expiry_hash_table_size=}"
                    )
                case b"\xfa":
                    # aux field
                    aux_key = self.read_length_encoded_integer()
                    aux_value = self.read_length_encoded_integer()
                    print(f"got aux field {aux_key=}, {aux_value=}")
                case _:
                    break
        print(f"{self.idx=}, {len(self.data)=}")
        return buffer
