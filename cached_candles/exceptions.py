from typing import Any

class APIError(Exception):
    """A general exception class to raise any API related errors."""
    
    def __init__(self, message: str = None, error: Any = None, **kwargs) -> None:
        self.error = error
        super(APIError, self).__init__(message, **kwargs)