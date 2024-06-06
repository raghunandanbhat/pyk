import unittest
import time
import random
import uuid
from src.format import (
    encode_header,
    decode_header,
    encode_kv,
    decode_kv,
    HEADER_SIZE,
)


class ForamtTester(unittest.TestCase):
    def test_header__encoder_decoder(self) -> None:
        tstamp, ksz, vsz = random_hdr()
        data = encode_header(tstamp, ksz, vsz)
        t, k, v = decode_header(data)

        self.assertEqual(tstamp, t)
        self.assertEqual(ksz, k)
        self.assertEqual(vsz, v)

    def test_headers(self):
        for _ in range(50):
            self.test_header__encoder_decoder()

    def test_kv_encoder_decoder(self) -> None:
        tstamp, key, val, size = random_kv_entry()
        sz, data = encode_kv(tstamp, key, val)
        t, k, v = decode_kv(data)

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
    return int(time.time()), random_gen(0, max_int), random_gen(0, max_int)


def random_kv_entry():
    return (
        int(time.time()),
        str(uuid.uuid1()),
        str(uuid.uuid4()),
        HEADER_SIZE + len(str(uuid.uuid1())) + len(str(uuid.uuid4())),
    )


if __name__ == "__main__":
    unittest.main()
