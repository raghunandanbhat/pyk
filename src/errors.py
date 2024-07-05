class UnsupportedTypeError(ValueError):
    def __init__(self, value_type: type, context: str = "") -> None:
        self.value_type = value_type
        self.context = context
        super().__init__(self.__str__())

    def __str__(self) -> str:
        message = f"Unsupported type: {self.value_type.__name__}"
        if self.context:
            message += f" ({self.context})"

        return message
