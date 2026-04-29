"""
Tests for the MVCC implementation.

Covers:
- Basic read/write/delete within a single transaction
- Snapshot isolation (reads see committed data at snapshot time)
- Write-write conflict detection and automatic abort
- Rollback correctness (aborted writes are not visible)
- Repeated reads within a transaction return consistent values
- Context manager (commit on success, abort on exception)
- Concurrent transactions via threads
- Delete semantics
"""

import threading
import time
import unittest

from mvcc import (
    MVCCDB,
    KeyNotFoundError,
    TransactionAbortedError,
    TxStatus,
    WriteConflictError,
)


class TestBasicOperations(unittest.TestCase):
    def setUp(self):
        self.db = MVCCDB()

    def test_set_and_get_within_same_transaction(self):
        with self.db.begin() as tx:
            tx.set("a", 1)
            self.assertEqual(tx.get("a"), 1)

    def test_get_nonexistent_key_raises(self):
        with self.db.begin() as tx:
            with self.assertRaises(KeyNotFoundError):
                tx.get("missing")

    def test_get_or_none_returns_none_for_missing(self):
        with self.db.begin() as tx:
            self.assertIsNone(tx.get_or_none("missing"))

    def test_set_overwrites_previous_value(self):
        with self.db.begin() as tx:
            tx.set("x", 10)
            tx.set("x", 20)
            self.assertEqual(tx.get("x"), 20)

    def test_delete_makes_key_invisible(self):
        with self.db.begin() as tx:
            tx.set("k", 99)

        with self.db.begin() as tx:
            tx.delete("k")
            with self.assertRaises(KeyNotFoundError):
                tx.get("k")

    def test_delete_nonexistent_key_raises(self):
        with self.db.begin() as tx:
            with self.assertRaises(KeyNotFoundError):
                tx.delete("nope")

    def test_keys_returns_visible_keys(self):
        with self.db.begin() as tx:
            tx.set("b", 2)
            tx.set("a", 1)
            self.assertEqual(tx.keys(), ["a", "b"])

    def test_committed_value_visible_to_later_transaction(self):
        with self.db.begin() as tx:
            tx.set("greeting", "hello")

        with self.db.begin() as tx:
            self.assertEqual(tx.get("greeting"), "hello")

    def test_transaction_xid_increments(self):
        tx1 = self.db.begin()
        tx2 = self.db.begin()
        tx1.commit()
        tx2.commit()
        self.assertGreater(tx2.xid, tx1.xid)


class TestSnapshotIsolation(unittest.TestCase):
    def setUp(self):
        self.db = MVCCDB()

    def test_transaction_does_not_see_uncommitted_write(self):
        """Reader should not see a value written by an active (uncommitted) writer."""
        with self.db.begin() as setup_tx:
            setup_tx.set("val", "initial")

        writer = self.db.begin()
        writer.set("val", "updated")

        reader = self.db.begin()
        # Writer is still active; reader must see "initial".
        self.assertEqual(reader.get("val"), "initial")

        writer.commit()
        # Reader's snapshot was taken before writer committed, so still "initial".
        self.assertEqual(reader.get("val"), "initial")
        reader.commit()

    def test_transaction_sees_own_writes(self):
        with self.db.begin() as tx:
            tx.set("k", "mine")
            self.assertEqual(tx.get("k"), "mine")

    def test_repeatable_read(self):
        """Reading the same key twice in a transaction must return the same value."""
        with self.db.begin() as setup_tx:
            setup_tx.set("counter", 0)

        tx = self.db.begin()
        first_read = tx.get("counter")

        # Another transaction commits a change.
        with self.db.begin() as other:
            other.set("counter", 999)

        second_read = tx.get("counter")
        self.assertEqual(first_read, second_read)
        tx.commit()

    def test_new_transaction_sees_committed_changes(self):
        """A transaction started after a commit must see the committed value."""
        with self.db.begin() as tx:
            tx.set("x", 42)

        with self.db.begin() as tx:
            self.assertEqual(tx.get("x"), 42)

    def test_snapshot_excludes_concurrent_transaction(self):
        """
        Two transactions start concurrently.  Neither should see the other's writes
        in their snapshots.
        """
        tx1 = self.db.begin()
        tx2 = self.db.begin()

        tx1.set("shared", "from-tx1")
        tx2.set("exclusive", "from-tx2")

        # tx1 must not see tx2's write.
        with self.assertRaises(KeyNotFoundError):
            tx1.get("exclusive")

        # tx2 must not see tx1's write.
        with self.assertRaises(KeyNotFoundError):
            tx2.get("shared")

        tx1.commit()
        tx2.commit()


class TestWriteConflicts(unittest.TestCase):
    def setUp(self):
        self.db = MVCCDB()

    def _seed(self, key, value):
        with self.db.begin() as tx:
            tx.set(key, value)

    def test_concurrent_writes_to_same_key_conflict(self):
        """Second writer on same key must raise WriteConflictError."""
        self._seed("balance", 100)

        tx1 = self.db.begin()
        tx2 = self.db.begin()

        tx1.set("balance", 90)
        tx1.commit()

        # tx2 started before tx1 committed — it should detect a conflict.
        with self.assertRaises(WriteConflictError):
            tx2.set("balance", 80)

        self.assertEqual(tx2.status, TxStatus.ABORTED)

    def test_conflicting_transaction_is_auto_aborted(self):
        self._seed("k", "v0")

        tx1 = self.db.begin()
        tx2 = self.db.begin()

        tx1.set("k", "v1")
        tx1.commit()

        try:
            tx2.set("k", "v2")
        except WriteConflictError:
            pass

        self.assertEqual(tx2.status, TxStatus.ABORTED)

    def test_aborted_transaction_cannot_be_used(self):
        tx = self.db.begin()
        tx.abort()

        with self.assertRaises(TransactionAbortedError):
            tx.get("anything")

        with self.assertRaises(TransactionAbortedError):
            tx.set("anything", 1)

    def test_committed_transaction_cannot_be_reused(self):
        tx = self.db.begin()
        tx.commit()

        with self.assertRaises(TransactionAbortedError):
            tx.get("x")

    def test_no_conflict_on_different_keys(self):
        """Writes to different keys must never conflict."""
        tx1 = self.db.begin()
        tx2 = self.db.begin()

        tx1.set("key1", "a")
        tx2.set("key2", "b")

        tx1.commit()
        tx2.commit()  # must not raise

        with self.db.begin() as r:
            self.assertEqual(r.get("key1"), "a")
            self.assertEqual(r.get("key2"), "b")


class TestRollback(unittest.TestCase):
    def setUp(self):
        self.db = MVCCDB()

    def test_aborted_write_not_visible(self):
        with self.db.begin() as tx:
            tx.set("val", "original")

        aborted_tx = self.db.begin()
        aborted_tx.set("val", "overwrite")
        aborted_tx.abort()

        with self.db.begin() as r:
            self.assertEqual(r.get("val"), "original")

    def test_aborted_delete_not_visible(self):
        with self.db.begin() as tx:
            tx.set("persistent", True)

        aborted_tx = self.db.begin()
        aborted_tx.delete("persistent")
        aborted_tx.abort()

        with self.db.begin() as r:
            self.assertTrue(r.get("persistent"))

    def test_context_manager_aborts_on_exception(self):
        with self.db.begin() as setup:
            setup.set("data", "safe")

        try:
            with self.db.begin() as tx:
                tx.set("data", "dangerous")
                raise RuntimeError("simulated error")
        except RuntimeError:
            pass

        with self.db.begin() as r:
            self.assertEqual(r.get("data"), "safe")

    def test_context_manager_commits_on_success(self):
        with self.db.begin() as tx:
            tx.set("committed", True)

        with self.db.begin() as r:
            self.assertTrue(r.get("committed"))


class TestConcurrency(unittest.TestCase):
    """Integration tests using real threads."""

    def setUp(self):
        self.db = MVCCDB()

    def test_many_readers_consistent_snapshot(self):
        """Multiple concurrent readers should all see the same committed value."""
        with self.db.begin() as tx:
            tx.set("shared", "stable")

        results = []
        errors = []

        def reader():
            try:
                with self.db.begin() as tx:
                    time.sleep(0.01)  # let other threads run
                    results.append(tx.get("shared"))
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=reader) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(errors, [])
        self.assertTrue(all(v == "stable" for v in results))
        self.assertEqual(len(results), 10)

    def test_write_conflict_resolution_under_threads(self):
        """
        Two threads compete to update the same counter.  Exactly one must succeed
        and the other must raise WriteConflictError.
        """
        with self.db.begin() as tx:
            tx.set("counter", 0)

        successes = []
        conflicts = []

        barrier = threading.Barrier(2)

        def updater(new_value):
            tx = self.db.begin()
            barrier.wait()  # both start at the same time
            try:
                tx.set("counter", new_value)
                tx.commit()
                successes.append(new_value)
            except WriteConflictError:
                conflicts.append(new_value)

        t1 = threading.Thread(target=updater, args=(1,))
        t2 = threading.Thread(target=updater, args=(2,))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        # Exactly one succeeds, one conflicts.
        self.assertEqual(len(successes), 1)
        self.assertEqual(len(conflicts), 1)

        with self.db.begin() as r:
            self.assertIn(r.get("counter"), [1, 2])

    def test_concurrent_independent_writes_all_committed(self):
        """Concurrent writers on different keys must all succeed."""
        successes = []

        def writer(key, val):
            with self.db.begin() as tx:
                tx.set(key, val)
            successes.append(key)

        threads = [threading.Thread(target=writer, args=(f"k{i}", i)) for i in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(len(successes), 20)

        with self.db.begin() as r:
            for i in range(20):
                self.assertEqual(r.get(f"k{i}"), i)


class TestEdgeCases(unittest.TestCase):
    def setUp(self):
        self.db = MVCCDB()

    def test_delete_then_recreate_in_same_transaction(self):
        with self.db.begin() as tx:
            tx.set("item", "first")

        with self.db.begin() as tx:
            tx.delete("item")
            tx.set("item", "second")
            self.assertEqual(tx.get("item"), "second")

        with self.db.begin() as r:
            self.assertEqual(r.get("item"), "second")

    def test_empty_database_has_no_keys(self):
        with self.db.begin() as tx:
            self.assertEqual(tx.keys(), [])

    def test_large_number_of_transactions(self):
        """Stress: 100 sequential transactions each incrementing a counter."""
        with self.db.begin() as tx:
            tx.set("n", 0)

        for _ in range(100):
            with self.db.begin() as tx:
                current = tx.get("n")
                tx.set("n", current + 1)

        with self.db.begin() as tx:
            self.assertEqual(tx.get("n"), 100)

    def test_multiple_keys_visibility_after_delete(self):
        with self.db.begin() as tx:
            tx.set("a", 1)
            tx.set("b", 2)
            tx.set("c", 3)

        with self.db.begin() as tx:
            tx.delete("b")

        with self.db.begin() as r:
            self.assertEqual(r.keys(), ["a", "c"])
            self.assertEqual(r.get("a"), 1)
            self.assertEqual(r.get("c"), 3)


if __name__ == "__main__":
    unittest.main(verbosity=2)
