from typing import Union

KeyType = Union[str, int, bytes]
ValueType = Union[str, int, bytes, float]

assert isinstance("name", KeyType)
assert isinstance(3, KeyType)
assert isinstance(b"hello", KeyType)
assert not isinstance([1, 2, 3], KeyType), "not a valid type for key"

assert isinstance("Joe", ValueType)
assert isinstance(3, ValueType)
assert isinstance(3.14, ValueType)
assert isinstance(b"World!", ValueType)
