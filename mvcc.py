"""
Multi-Version Concurrency Control (MVCC) implementation.

MVCC allows concurrent transactions to operate on the database without blocking
each other. Each transaction sees a consistent snapshot of the data as it existed
at the start of that transaction. Writers do not block readers and readers do not
block writers.

Key concepts:
- Transaction ID (xid): monotonically increasing integer assigned to each transaction.
- Version: a record of a value at a specific point in time, tagged with xmin (the
  transaction that created it) and xmax (the transaction that deleted/superseded it,
  or None if still live).
- Snapshot: the set of committed transaction IDs visible to a given transaction, plus
  the transaction's own xid.
- Isolation level: SNAPSHOT ISOLATION is implemented here (sometimes called
  Repeatable Read in PostgreSQL). Write-write conflicts (lost updates) are detected
  and cause the later writer to abort.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Dict, List, Optional, Set, Tuple


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class TransactionAbortedError(Exception):
    """Raised when a transaction has already been aborted."""


class WriteConflictError(TransactionAbortedError):
    """Raised when two concurrent transactions write to the same key."""


class KeyNotFoundError(KeyError):
    """Raised when a key does not exist in the current snapshot."""


# ---------------------------------------------------------------------------
# Transaction state
# ---------------------------------------------------------------------------


class TxStatus(Enum):
    ACTIVE = auto()
    COMMITTED = auto()
    ABORTED = auto()


# ---------------------------------------------------------------------------
# Version record
# ---------------------------------------------------------------------------


@dataclass
class Version:
    """One version of a value for a given key."""

    value: Any
    xmin: int          # transaction that created this version
    xmax: Optional[int] = None  # transaction that deleted this version (None = live)


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------


class MVCCDB:
    """
    An in-memory key/value store with MVCC snapshot isolation.

    Usage::

        db = MVCCDB()

        tx1 = db.begin()
        tx1.set("x", 1)
        tx1.commit()

        tx2 = db.begin()
        print(tx2.get("x"))   # 1
        tx2.commit()
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()

        # Next transaction ID counter.
        self._next_xid: int = 1

        # Transaction status registry.
        self._tx_status: Dict[int, TxStatus] = {}

        # Data store: key -> list of versions (ordered oldest to newest).
        self._data: Dict[str, List[Version]] = {}

        # Set of committed xids (used for snapshot computation).
        self._committed: Set[int] = set()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _alloc_xid(self) -> int:
        xid = self._next_xid
        self._next_xid += 1
        self._tx_status[xid] = TxStatus.ACTIVE
        return xid

    def _committed_snapshot(self, before_xid: int) -> Set[int]:
        """Return all xids that committed strictly before *before_xid* was allocated."""
        return {x for x in self._committed if x < before_xid}

    def _version_visible(self, ver: Version, snapshot: Set[int], xid: int) -> bool:
        """
        Return True if *ver* is visible to a transaction with the given *snapshot*
        and transaction ID *xid*.

        A version is visible when:
        1. Its creator (xmin) is committed and in the snapshot, OR the creator is
           the current transaction itself.
        2. It has not been deleted (xmax is None), OR its deletion was made by a
           transaction that is NOT committed in the snapshot and is NOT the current
           transaction.
        """
        # Check creator visibility.
        xmin_visible = (ver.xmin == xid) or (ver.xmin in snapshot)
        if not xmin_visible:
            return False

        # Check deletion visibility.
        if ver.xmax is None:
            return True
        xmax_visible = (ver.xmax == xid) or (ver.xmax in snapshot)
        return not xmax_visible

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def begin(self) -> "Transaction":
        """Start a new transaction and return a handle to it."""
        with self._lock:
            xid = self._alloc_xid()
            snapshot = self._committed_snapshot(xid)
        return Transaction(db=self, xid=xid, snapshot=snapshot)

    # ------------------------------------------------------------------
    # Methods called by Transaction (package-internal)
    # ------------------------------------------------------------------

    def _get(self, key: str, snapshot: Set[int], xid: int) -> Any:
        with self._lock:
            versions = self._data.get(key, [])
            # Walk newest-first to find the latest visible version.
            for ver in reversed(versions):
                if self._version_visible(ver, snapshot, xid):
                    return ver.value
        raise KeyNotFoundError(key)

    def _set(self, key: str, value: Any, snapshot: Set[int], xid: int) -> None:
        with self._lock:
            versions = self._data.setdefault(key, [])

            # Detect write-write conflict: if any live version was written by a
            # concurrent committed transaction (one that committed after our snapshot
            # was taken), abort immediately.
            for ver in reversed(versions):
                if ver.xmax is None and ver.xmin != xid:
                    if ver.xmin not in snapshot:
                        # The version was created by a transaction we cannot see —
                        # either it committed after our snapshot or is still active.
                        # Either way this is a write-write conflict.
                        raise WriteConflictError(
                            f"Write conflict on key '{key}': "
                            f"concurrent transaction {ver.xmin} holds a live version."
                        )

            # Logically delete the current live version (if any) by stamping xmax.
            for ver in reversed(versions):
                if self._version_visible(ver, snapshot, xid) and ver.xmax is None:
                    ver.xmax = xid
                    break

            versions.append(Version(value=value, xmin=xid))

    def _delete(self, key: str, snapshot: Set[int], xid: int) -> None:
        with self._lock:
            versions = self._data.get(key, [])
            for ver in reversed(versions):
                if self._version_visible(ver, snapshot, xid) and ver.xmax is None:
                    # Detect write-write conflict (same logic as _set).
                    if ver.xmin != xid and ver.xmin not in snapshot:
                        raise WriteConflictError(
                            f"Write conflict on key '{key}': "
                            f"concurrent transaction {ver.xmin} holds a live version."
                        )
                    ver.xmax = xid
                    return
        raise KeyNotFoundError(key)

    def _keys(self, snapshot: Set[int], xid: int) -> List[str]:
        with self._lock:
            result = []
            for key, versions in self._data.items():
                for ver in reversed(versions):
                    if self._version_visible(ver, snapshot, xid):
                        result.append(key)
                        break
            return sorted(result)

    def _commit(self, xid: int, write_set: Set[str]) -> None:
        with self._lock:
            self._tx_status[xid] = TxStatus.COMMITTED
            self._committed.add(xid)

    def _abort(self, xid: int, write_set: Set[str]) -> None:
        with self._lock:
            self._tx_status[xid] = TxStatus.ABORTED
            # Roll back all versions written by this transaction.
            for key in write_set:
                versions = self._data.get(key, [])
                # Remove versions created by xid.
                self._data[key] = [v for v in versions if v.xmin != xid]
                # Restore xmax stamps set by xid (un-delete).
                for ver in self._data[key]:
                    if ver.xmax == xid:
                        ver.xmax = None


# ---------------------------------------------------------------------------
# Transaction handle
# ---------------------------------------------------------------------------


class Transaction:
    """
    Handle to an active transaction.  All reads and writes go through this object.

    Do not instantiate directly; use :meth:`MVCCDB.begin` instead.
    """

    def __init__(self, db: MVCCDB, xid: int, snapshot: Set[int]) -> None:
        self._db = db
        self._xid = xid
        self._snapshot = snapshot
        self._status = TxStatus.ACTIVE
        self._write_set: Set[str] = set()

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def xid(self) -> int:
        return self._xid

    @property
    def status(self) -> TxStatus:
        return self._status

    # ------------------------------------------------------------------
    # Guard
    # ------------------------------------------------------------------

    def _ensure_active(self) -> None:
        if self._status == TxStatus.COMMITTED:
            raise TransactionAbortedError(f"Transaction {self._xid} is already committed.")
        if self._status == TxStatus.ABORTED:
            raise TransactionAbortedError(f"Transaction {self._xid} has been aborted.")

    # ------------------------------------------------------------------
    # Data operations
    # ------------------------------------------------------------------

    def get(self, key: str) -> Any:
        """Read the value of *key* as seen by this transaction's snapshot."""
        self._ensure_active()
        return self._db._get(key, self._snapshot, self._xid)

    def set(self, key: str, value: Any) -> None:
        """Write *value* for *key*. Raises :exc:`WriteConflictError` on conflict."""
        self._ensure_active()
        try:
            self._db._set(key, value, self._snapshot, self._xid)
        except WriteConflictError:
            self._do_abort()
            raise
        self._write_set.add(key)

    def delete(self, key: str) -> None:
        """Delete *key*. Raises :exc:`WriteConflictError` on conflict."""
        self._ensure_active()
        try:
            self._db._delete(key, self._snapshot, self._xid)
        except WriteConflictError:
            self._do_abort()
            raise
        self._write_set.add(key)

    def keys(self) -> List[str]:
        """Return a sorted list of all keys visible to this transaction."""
        self._ensure_active()
        return self._db._keys(self._snapshot, self._xid)

    def get_or_none(self, key: str) -> Optional[Any]:
        """Like :meth:`get` but returns ``None`` if the key does not exist."""
        try:
            return self.get(key)
        except KeyNotFoundError:
            return None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def commit(self) -> None:
        """Commit the transaction, making all writes permanent."""
        self._ensure_active()
        self._db._commit(self._xid, self._write_set)
        self._status = TxStatus.COMMITTED

    def abort(self) -> None:
        """Abort (roll back) the transaction."""
        self._ensure_active()
        self._do_abort()

    def _do_abort(self) -> None:
        self._db._abort(self._xid, self._write_set)
        self._status = TxStatus.ABORTED

    # ------------------------------------------------------------------
    # Context manager support
    # ------------------------------------------------------------------

    def __enter__(self) -> "Transaction":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        if self._status == TxStatus.ACTIVE:
            if exc_type is None:
                self.commit()
            else:
                self._do_abort()
        return False  # do not suppress exceptions

    def __repr__(self) -> str:
        return f"Transaction(xid={self._xid}, status={self._status.name})"
