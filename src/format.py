import typing
import struct
import zlib
import time
from src.custom_types import KeyType, ValueType

"""
ref: https://riak.com/assets/bitcask-intro.pdf

For each key value pair written to disk should be formatted in the following
way

    | crc | timestamp | expirey | ksz | value_sz |..key..|..value..|
    | <-----------------HEADER-----------------> | <-----DATA----> |

this is a stream of bytes written to the file. All writes to the file are
appended at the end. The data file is linear sequence of 'KVEntry' entries.
key and value can have varibale length.

"""

# header is packed into binary data using strcut.pack
# most machines use little-endian representation - denoted by '<'
# L represents the unsigned-long representing the size of values
# being encoded. Since CRC, timestamp, expiry, ksz, value_sz - 5 values are encoded,
# three L's are present in the HEADER_ENCODING_FORMAT string
HEADER_ENCODING_FORAMT: typing.Final[str] = "<LLLLL"

# size of the HEADER. Five values, each of size 4 bytes, totaling 20 bytes.
HEADER_SIZE: typing.Final[int] = 20


class KVHeader:
    """
    KVHeader to store format header
    args:
        checksum  : CRC32 checksum for data integrity check
        timestamp : timestamp at which key value pair is written to the disk
        expirey   : TTL for key value pair
    """

    def __init__(
        self,
        checksum: int,
        timestamp: int,
        key_sz: int,
        value_sz: int,
        expirey: int = 0,
    ):
        self.checksum = checksum
        self.timestamp = timestamp
        self.expirey = expirey
        self.key_sz = key_sz
        self.value_sz = value_sz

    def encode(self) -> bytes:
        """
        encode header into bytes using encoding format

        args:
            timestamp : timestamp of writing of KV pair in the disk
            key_sz    : size of the key
            value_sz  : size of value filed

        returns bytes object conatining encoded header data
        """
        return struct.pack(
            HEADER_ENCODING_FORAMT,
            self.checksum,
            self.timestamp,
            self.expirey,
            self.key_sz,
            self.value_sz,
        )

    @classmethod
    def decode(cls, data: bytes) -> tuple[int, int, int, int, int]:
        """
        decode header bytes into header using the encoding format

        args:
            data : byte object conatining encoded header data

        returns a tuple of timestamp, key_sz and value_sz
        """
        # print(len(data), data)
        return struct.unpack(HEADER_ENCODING_FORAMT, data)

    def expired(self) -> bool:
        if self.expirey == 0:
            return False
        return self.expirey <= int(time.time())

    def valid(self, value: ValueType) -> bool:
        return self.checksum == zlib.crc32(str(value).encode("utf-8"))


class KVData:
    def __init__(self, header: KVHeader, key: KeyType, value: ValueType):
        self.header = header
        self.key = key
        self.value = value

    def encode_kv(self) -> tuple[int, bytes]:
        """
        encodes KV pair into bytes object containing header plus data

        args:
            timestamp : timestamp when the KV pair is written to disk
            key       : key to be written to disk
            value     : value to be written to disk

        returns a tuple of size of encoded bytes and byte object
        """
        hdr: bytes = self.header.encode()
        data: bytes = b"".join([str.encode(self.key), str.encode(self.value)])
        return HEADER_SIZE + len(data), hdr + data

    @classmethod
    def decode_kv(cls, data: bytes) -> tuple[int, KVHeader, KeyType, ValueType]:
        """
        decode byte object into timestamp, key and value

        args:
            data : byte object containing KV pair data

        returns a tuple of timestamp, key and value
        """
        chksm, timestamp, expirey, key_sz, value_sz = KVHeader.decode(
            data[:HEADER_SIZE]
        )
        hdr = KVHeader(
            checksum=chksm,
            timestamp=timestamp,
            expirey=expirey,
            key_sz=key_sz,
            value_sz=value_sz,
        )
        key = data[HEADER_SIZE : HEADER_SIZE + key_sz].decode("utf-8")
        value = data[HEADER_SIZE + key_sz :].decode("utf-8")
        return timestamp, hdr, key, value


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
