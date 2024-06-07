import time
import os
from os import path, fsync
from src.format import (
    KVEntry,
    decode_header,
    encode_kv,
    decode_kv,
    HEADER_SIZE,
)


class DiskStorage:
    def __init__(self, filename: str = "file.db"):
        self.filename: str = filename
        self.write_pos: int = 0
        self.key_dir: dict[str, KVEntry] = {}

        if path.exists(filename):
            self._init_key_dir()

        self.file = open(filename, "a+b")

    def set(self, key: str, value: str) -> None:
        """
        store key and value on disk
        args:
            key   : the key
            value : corresponding value
        """
        tstamp: int = int(time.time())
        sz, data = encode_kv(tstamp, key, value)
        self._write(data)

        kv_entry: KVEntry = KVEntry(tstamp, self.write_pos, sz)

        self.key_dir[key] = kv_entry
        self.write_pos += sz

    def get(self, key: str) -> str:
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
        _, _, value = decode_kv(data)
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

        with open(self.filename, "rb") as f:
            while hdr_bytes := f.read(HEADER_SIZE):
                tstamp, ksz, vsz = decode_header(hdr_bytes)

                key_bytes = f.read(ksz)
                key = key_bytes.decode("utf-8")

                # advance seek position by vsz position so that we are reading
                # the next correct KV entry
                f.seek(vsz, os.SEEK_CUR)

                total_size = HEADER_SIZE + ksz + vsz
                kv_entry = KVEntry(tstamp, self.write_pos, total_size)

                self.key_dir[key] = kv_entry
                self.write_pos += total_size
        print("db initialization comeplete, ready to use!\n")
