import os
import tempfile
import unittest

from src.disk_store import KVStore


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
            self.assertEqual(ds.get(k), "")
            self.assertNotEqual(ds.get(k), v)

        ds.close()


if __name__ == "__main__":
    unittest.main()
