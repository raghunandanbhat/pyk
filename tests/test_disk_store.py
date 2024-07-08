import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.disk_store import KVStore
from src.errors import UnsupportedTypeError


class TempStorageFile:
    """
    Wrapper class for python tempfile, which are used for testing the db
    """

    def __init__(self):
        fd, self.path = tempfile.mkstemp()
        os.close(fd)

    def cleanup(self):
        """
        deletes tempfiles created for db testing
        """
        os.remove(self.path)
        assert not os.path.exists(
            self.path
        ), f"""could not delete tempfile at {self.path}, please delete them manually!"""


class TestDiskStorage(unittest.TestCase):
    def setUp(self):
        """
        *do not move this functions*
        cerates a tempfile for database testing
        """
        self.file = TempStorageFile()
        assert self.file.path, "tempfile creation failed"

    def tearDown(self):
        """
        * do not remove this file *
        clean up tempfiles after tests are done
        """
        self.file.cleanup()

    def test_get(self):
        ds = KVStore(self.file.path)
        ds.set(key="foo", value="bar")

        self.assertEqual(ds.get("foo"), "bar")
        ds.close()

    def test_get_invalid_key(self):
        ds = KVStore(self.file.path)

        self.assertEqual(ds.get("bar"), "")

        ds.close()

    def test_set(self):
        ds = KVStore(self.file.path)
        ds.set("quick", "brown fox")
        self.assertEqual(ds.get("quick"), "brown fox")

        ds.set("quick", "black fox")
        self.assertNotEqual(ds.get("quick"), "brown fox")
        self.assertNotEqual(ds.get("quick"), "")
        self.assertEqual(ds.get("quick"), "black fox")

        kvs = {
            "name": "Alice",
            "city": "New York",
            "age": 30,
            "pay_rate": 75.5,
            "occupation": "Engineer",
            "hobbies": "Reading, Cycling, Hiking",
            "email": b"alice@example.com",
            b"languages": "English, Spanish, French",
            1234: "id",
            3.14: b"PI",
        }

        for k, v in kvs.items():
            ds.set(k, v)
            val = ds.get(k)
            if isinstance(val, type(v)):
                self.assertEqual(val, v)
            else:
                if isinstance(v, int):
                    self.assertEqual(int(val), v)
                elif isinstance(v, float):
                    self.assertEqual(float(val), v)
                elif isinstance(v, bytes):
                    self.assertEqual(val, v.decode("utf-8"))
                else:
                    pass

        ds.close()

    def test_persistance(self):
        ds = KVStore(self.file.path)

        kvs = {
            "name": "Alice",
            "city": "New York",
            "age": "30",
            "occupation": "Engineer",
            "hobbies": "Reading, Cycling, Hiking",
            "email": "alice@example.com",
            "languages": "English, Spanish, French",
        }

        for k, v in kvs.items():
            ds.set(k, v)
            self.assertEqual(ds.get(k), v)

        ds.close()

        ds = KVStore(self.file.path)
        for k, v in kvs.items():
            self.assertEqual(ds.get(k), v)

        ds.close()

    def test_delete(self):
        ds = KVStore(self.file.path)

        kvs = {
            "name": "Alice",
            "city": "New York",
            "age": "30",
            "occupation": "Engineer",
            "hobbies": "Reading, Cycling, Hiking",
            "email": "alice@example.com",
            "languages": "English, Spanish, French",
        }

        for k, v in kvs.items():
            ds.set(k, v)
            self.assertEqual(ds.get(k), v)

        for k, v in kvs.items():
            ds.delete(k)
            self.assertEqual(ds.get(k), "Key deleted")
            self.assertNotEqual(ds.get(k), v)

        ds.close()

    def test_UnsupportedTypeError(self):
        ds = KVStore(self.file.path)

        kvs = {"alist": [1, 2, 3], (1, 2): "tuple"}

        with self.assertRaises(UnsupportedTypeError):
            for k, v in kvs.items():
                ds.set(k, v)


if __name__ == "__main__":
    unittest.main()
