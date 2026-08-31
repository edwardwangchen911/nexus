"""Sanitized local transport refusals that provider retry logic must not catch."""


class NonRetryableTransportRefusal(RuntimeError):
    """A local call-edge refusal that must never enter provider retry logic."""
