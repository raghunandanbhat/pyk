import os
import time
import zlib
from os import fsync, path

from src.compact import shrink
from src.custom_types import TOMBSTONE, KeyType, ValueType
from src.errors import UnsupportedTypeError
from src.format import (
    HEADER_SIZE,
    KVData,
    KVEntry,
    KVHeader,
)
from src.utils import encode_to_str


class KVStore:
    def __init__(self, filename: str = "file.db"):
        self.filename: str = filename
        self.write_pos: int = 0
        self.key_dir: dict[str, KVEntry] = {}

        if path.exists(filename):
            self._init_key_dir()

        self.file = open(filename, "a+b")

    def set(self, key: KeyType, value: ValueType, expiry: int = 0) -> None:
        """
        store key and value on disk
        args:
            key    : the key
            value  : corresponding value
            expiry : key value expiry time in seconds
        """
        self._set_key(
            key=key,
            val=value,
            expiry=expiry,
        )

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
        try:
            key: str = encode_to_str(key)
        except UnsupportedTypeError as e:
            raise UnsupportedTypeError(e.value_type, "for key in get()") from e

        kv_entry = self.key_dir.get(key, None)
        if not kv_entry:
            return "Key Not Found"

        self.file.seek(kv_entry.pos, os.SEEK_SET)
        data: bytes = self.file.read(kv_entry.size)
        _, hdr, _, value = KVData.decode_kv(data)

        # check for TTL expirey
        # if expired, delete it
        if hdr.is_expired():
            self.delete(key)
            return "Key Not Found"

        # check for deleted key:
        if hdr.is_deleted():
            return "Key Not Found"

        # verify CRC checksum
        if not hdr.is_valid(value):
            return "Invalid/corrupted"

        return value

    def delete(self, key: str) -> None:
        """
        deletes a given key
        delete operations calls the set operation and sets the value to an
        empty string.
        args:
            key : key to be deleted
        """
        self._set_key(
            key=key,
            val=TOMBSTONE,
            mark_delete=True,
        )

    def close(self) -> None:
        self.file.flush()
        fsync(self.file.fileno())
        self.file.close()

        # run compaction to remove deleted/expired keys
        shrink(self.filename)

    def _set_key(
        self,
        key: KeyType,
        val: ValueType,
        expiry: int = 0,
        mark_delete: bool = False,
    ) -> None:
        """
        set a value for key, persist to disk. when a key is deleted,
        a tombstone value is written by calling this function
        """
        try:
            key: str = encode_to_str(key)
        except UnsupportedTypeError as e:
            raise UnsupportedTypeError(
                e.value_type,
                "for key in _set_key()",
            ) from e

        try:
            val: str = encode_to_str(val)
        except UnsupportedTypeError as e:
            raise UnsupportedTypeError(
                e.value_type,
                "for value in _set_key()",
            ) from e

        tstamp: int = int(time.time())
        expiry_tstmap: int = (tstamp + expiry) if expiry > 0 else expiry
        crc32_checksum: int = zlib.crc32(str(val).encode("utf-8"))

        kv_header = KVHeader(
            checksum=crc32_checksum,
            timestamp=tstamp,
            expiry=expiry_tstmap,
            deleted=1 if mark_delete else 0,
            key_sz=len(str(key)),
            value_sz=len(str(val)),
        )

        sz, data = KVData(header=kv_header, key=key, value=val).encode_kv()
        self._write(data)
        kv_entry: KVEntry = KVEntry(
            timestamp=tstamp,
            pos=self.write_pos,
            size=sz,
        )
        self.key_dir[key] = kv_entry
        self.write_pos += sz

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
            steps involved
            1. flush to persist leftover data in buffers
            2. Load key_dir
        """
        print("initialing database...")

        with open(self.filename, "a+b") as f:
            # flushing buffers before reload to persit leftover data in buffers
            f.flush()
            fsync(f.fileno())
            # reset position back to zero
            f.seek(0)

            while hdr_bytes := f.read(HEADER_SIZE):
                chksm, tstamp, expiry, deleted, ksz, vsz = KVHeader.decode_hdr(
                    hdr_bytes,
                )

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
