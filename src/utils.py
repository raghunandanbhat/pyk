from src.custom_types import KeyType, ValueType


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
