from src.errors import UnsupportedTypeError


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
        raise UnsupportedTypeError(type(value), "in encode_to_str()")
