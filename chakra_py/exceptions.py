class ChakraAPIError(Exception):
    """Custom exception for Chakra API errors."""

    def __init__(self, message: str, response=None):
        self.message = message
        self.response = response
        super().__init__(self.message)
