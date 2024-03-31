BUFFER_SIZE = 1024
CONN_TIMEOUT = 15

OK_SIMPLE_STRING = "+OK\r\n"
NULL_BULK_STRING = "$-1\r\n"
STREAM_ID_NOT_GREATER_ERROR = "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"
STREAM_ID_TOO_SMALL_ERROR = "-ERR The ID specified in XADD must be greater than 0-0\r\n"
