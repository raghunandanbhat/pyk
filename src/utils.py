from src.custom_types import KeyType, ValueType
from src.errors import UnsupportedTypeError


def validate_kv(key: KeyType, value: ValueType) -> bool:
    if not isinstance(key, KeyType):
        return False

    if not isinstance(value, ValueType):
        return False

    if isinstance(key, int):
        key = str(key)

    if len(key) <= 0:
        return False

    return True


def encode_to_str(value) -> str:
    if isinstance(value, str):
        return value
    elif isinstance(value, (int, float)):
        return str(value)
    elif isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            return value.hex()
    else:
        raise UnsupportedTypeError(type(value), "in encode_to_str")
