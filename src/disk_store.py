import time
import os
import zlib
from os import path, fsync
from src.utils import validate_kv
from src.custom_types import KeyType, ValueType
from src.format import (
    KVEntry,
    KVHeader,
    KVData,
    HEADER_SIZE,
)


class KVStore:
    def __init__(self, filename: str = "file.db"):
        self.filename: str = filename
        self.write_pos: int = 0
        self.key_dir: dict[str, KVEntry] = {}

        if path.exists(filename):
            self._init_key_dir()

        self.file = open(filename, "a+b")

    def set(self, key: KeyType, value: ValueType, expirey: int = 0) -> None:
        """
        store key and value on disk
        args:
            key    : the key
            value  : corresponding value
            expirey: key value expirey time in seconds
        """
        if not validate_kv(key, value):
            return ""

        tstamp: int = int(time.time())
        crc32_checksum: int = zlib.crc32(str(value).encode("utf-8"))

        kv_header = KVHeader(
            checksum=crc32_checksum,
            timestamp=tstamp,
            expirey=(tstamp + expirey) if expirey > 0 else 0,
            key_sz=len(str(key)),
            value_sz=len(str(value)),
        )

        sz, data = KVData(header=kv_header, key=key, value=value).encode_kv()

        self._write(data)

        kv_entry: KVEntry = KVEntry(timestamp=tstamp, pos=self.write_pos, size=sz)

        self.key_dir[key] = kv_entry
        self.write_pos += sz

    def get(self, key: KeyType) -> str:
        """
        retrive value corresponding to a given key
            1. move the file pointer to the approprate postion
            2. read the exact number of bytes starting from the position set
               by the file pointer
            3. decode the bytes and return value
        args:
            key : the key to be retrived from the disk

        return value corresponding to given key if it exists, else empty string
        """
        kv_entry = self.key_dir.get(key, None)
        if not kv_entry:
            return ""

        self.file.seek(kv_entry.pos, os.SEEK_SET)
        data: bytes = self.file.read(kv_entry.size)
        _, hdr, _, value = KVData.decode_kv(data)

        # check for TTL expirey
        if hdr.expired():
            return ""

        # verify CRC checksum
        if not hdr.valid(value):
            return ""

        return value

    def delete(self, key: str) -> None:
        """
        deletes a given key
        delete operations calls the set operation and sets the value to an
        empty string.
        args:
            key : key to be deleted
        """
        return self.set(key, "")

    def close(self) -> None:
        self.file.flush()
        fsync(self.file.fileno())
        self.file.close()

    def _write(self, data: bytes) -> None:
        """
        writes bytes of data to the file and flush it to os buffer
        call fsync to persist the data to the disk
        """
        self.file.write(data)
        self.file.flush()
        fsync(self.file.fileno())

    def _init_key_dir(self) -> None:
        """
        loads the key_dir by reading the data files
        """
        print("initialing database...")

        with open(self.filename, "a+b") as f:
            # flushing buffers before reload to persit leftover data in buffers
            f.flush()
            fsync(f.fileno())
            # reset position back to zero
            f.seek(0)

            while hdr_bytes := f.read(HEADER_SIZE):
                chksm, tstamp, expiry, ksz, vsz = KVHeader.decode(hdr_bytes)

                key_bytes = f.read(ksz)
                key = key_bytes.decode("utf-8")

                # advance seek position by vsz position so that we are reading
                # the next correct KV entry
                f.seek(vsz, os.SEEK_CUR)

                total_size = HEADER_SIZE + ksz + vsz
                kv_entry = KVEntry(tstamp, self.write_pos, total_size)

                self.key_dir[key] = kv_entry
                self.write_pos += total_size
                # print(f"kv init for key-{key} complete..")
        print("db initialization comeplete, ready to use!\n")
