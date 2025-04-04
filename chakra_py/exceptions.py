class ChakraAPIError(Exception):
    """Custom exception for Chakra API errors."""

    def __init__(self, message: str, response=None):
        self.message = message
        self.response = response
        super().__init__(self.message)


class ChakraAuthError(ChakraAPIError):
    """Custom exception for Chakra authentication errors."""

    def __init__(self, message: str, response=None):
        super().__init__(message, response)
