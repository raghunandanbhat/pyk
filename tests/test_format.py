import os
import sys
import unittest
import time
import random
import uuid

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.format import (
    KVHeader,
    KVData,
    HEADER_SIZE,
)


class FormatTester(unittest.TestCase):
    def test_header_encoder_decoder(self) -> None:
        chksm, tstamp, expiry, deleted, ksz, vsz = random_hdr()
        hdr = KVHeader(
            checksum=chksm, timestamp=tstamp, key_sz=ksz, value_sz=vsz, expiry=expiry
        )
        data = hdr.encode_hdr()
        c, t, e, d, k, v = KVHeader.decode_hdr(data)

        self.assertEqual(tstamp, t)
        self.assertEqual(ksz, k)
        self.assertEqual(vsz, v)
        self.assertEqual(chksm, c)
        self.assertEqual(expiry, e)
        self.assertEqual(deleted, d)

    def test_headers(self):
        for _ in range(50):
            self.test_header_encoder_decoder()

    def test_kv_encoder_decoder(self) -> None:
        chksm, tstamp, expiry, deleted, key, val, size = random_kv_entry()

        hdr = KVHeader(
            checksum=chksm,
            timestamp=tstamp,
            key_sz=len(str(key)),
            value_sz=len(str(val)),
            expiry=expiry,
            deleted=deleted,
        )
        sz, data = KVData(header=hdr, key=key, value=val).encode_kv()
        t, hdr, k, v = KVData.decode_kv(data)

        self.assertEqual(sz, size)
        self.assertEqual(tstamp, t)
        self.assertEqual(key, k)
        self.assertEqual(val, v)

    def test_kv(self):
        for _ in range(50):
            self.test_kv_encoder_decoder()


def random_hdr():
    max_int = 2**32 - 1
    random_gen = random.randint
    return (
        random_gen(0, max_int),
        int(time.time()),
        int(time.time()) + 1000,
        0,
        random_gen(0, max_int),
        random_gen(0, max_int),
    )


def random_kv_entry():
    return (
        random.randint(0, (2**32 - 1)),
        int(time.time()),
        int(time.time()) + 1000,
        0,
        str(uuid.uuid1()),
        str(uuid.uuid4()),
        HEADER_SIZE + len(str(uuid.uuid1())) + len(str(uuid.uuid4())),
    )


if __name__ == "__main__":
    unittest.main()
