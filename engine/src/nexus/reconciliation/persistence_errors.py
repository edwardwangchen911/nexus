"""Sanitized broker synchronization persistence errors."""


class BrokerSyncPersistenceError(RuntimeError):
    """Base for sanitized broker synchronization persistence failures."""


class IdempotencyConflict(BrokerSyncPersistenceError):
    """The same request identity was reused with a different body hash."""


class SyncCooldownActive(BrokerSyncPersistenceError):
    """A successful manual synchronization was accepted less than 60 seconds ago."""


class SnapshotNotProposalEligible(BrokerSyncPersistenceError):
    """The portfolio has no currently authorized latest READY baseline."""
