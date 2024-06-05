import typing
import struct

"""
ref: https://riak.com/assets/bitcask-intro.pdf

For each key value pair written to disk should be formatted in the following
way

    | timestamp | ksz | value_sz |..key..|..value..|

this is a stream of bytes written to the file. All writes to the file are
appended at the end. The data file is linear sequence of 'KVEntry' entries.
key and value can have varibale length.

"""

# header is packed into binary data using strcut.pack
# most machines use little-endian representation - denoted by '<'
# L represents the unsigned-long representing the size of values
# being encoded. Since timestamp, ksz, value_sz - 3 values are encoded,
# three L's are present in the HEADER_ENCODING_FORMAT string
HEADER_ENCODING_FORAMT: typing.Final[str] = "<LLL"

# size of the HEADER. Three values, each of size 4 bytes, totaling 12 bytes.
HEADER_SIZE: typing.Final[int] = 12


class KVEntry:
    """
    KVEntry stores the metadat about KV pairs - timestampm of the entry, size
    and position of the byte offset in the file.
    A new entry is made whenever a key is inerted or updated

    args:
        timestamp : timestamp at which key value pair is written to the disk
        pos       : byte offset in the file
        size      : size of an entry in the file
    """

    def __init__(self, timestamp: int, pos: int, size: int):
        self.timestamp = timestamp
        self.pos = pos
        self.size = size


def encode_header(timestamp: int, key_sz: int, value_sz: int) -> bytes:
    """
    encode header into bytes using encoding format

    args:
        timestamp : timestamp of writing of KV pair in the disk
        key_sz    : size of the key
        value_sz  : size of value filed

    returns bytes object conatining encoded header data
    """
    return struct.pack(HEADER_ENCODING_FORAMT, timestamp, key_sz, value_sz)


def decode_header(data: bytes) -> tuple[int, int, int]:
    """
    decode header bytes into header using the encoding format

    args:
        data : byte object conatining encoded header data

    returns a tuple of timestamp, key_sz and value_sz
    """
    return struct.unpack(HEADER_ENCODING_FORAMT, data)


def encode_kv(timestamp: int, key: str, value: str) -> tuple[int, bytes]:
    """
    encodes KV pair into bytes object containing header plus data

    args:
        timestamp : timestamp when the KV pair is written to disk
        key       : key to be written to disk
        value     : value to be written to disk

    returns a tuple of size of encoded bytes and byte object
    """
    hdr = encode_header(timestamp, len(key), len(value))
    data = b"".join([str.encode(key), str.encode(value)])
    return HEADER_SIZE + len(data), hdr + data


def decode_kv(data: bytes) -> tuple[int, str, str]:
    """
    decode byte object into timestamp, key and value

    args:
        data : byte object containing KV pair data

    returns a tuple of timestamp, key and value
    """
    timestamp, key_sz, value_sz = decode_header(data[:HEADER_SIZE])
    key = data[HEADER_SIZE : HEADER_SIZE + key_sz].decode("utf-8")
    value = data[HEADER_SIZE + key_sz :].decode("utf-8")
    return timestamp, key, value
