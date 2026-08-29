"""Sanitized worker errors shared by closed Job handlers."""


class RetryableJobError(RuntimeError):
    """A handler failed with a registry-approved retryable code."""

    def __init__(self, error_code: str) -> None:
        super().__init__(error_code)
        self.error_code = error_code


class UnsafeJobError(RuntimeError):
    """A handler detected an authority or capability violation."""

    def __init__(self, error_code: str) -> None:
        super().__init__(error_code)
        self.error_code = error_code
