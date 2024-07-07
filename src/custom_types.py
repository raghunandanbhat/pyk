from typing import Union

TOMBSTONE = 0xDEADBEE

KeyType = Union[str, int, bytes, float]
ValueType = Union[str, int, bytes, float]

assert isinstance("name", KeyType)
assert isinstance(3, KeyType)
assert isinstance(b"hello", KeyType)
assert not isinstance([1, 2, 3], KeyType), "not a valid type for key"

assert isinstance("Joe", ValueType)
assert isinstance(3, ValueType)
assert isinstance(3.14, ValueType)
assert isinstance(b"World!", ValueType)
assert isinstance(TOMBSTONE, ValueType)
