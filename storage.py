# SHORT-LIVED STATISTICS CONNECTIONS
# SQLite connections are created and closed in their calling thread. The
# shared lock serializes compound analytics/similarity transactions; it does
# not authorize sharing a connection across threads. Frontier owns a separate
# connection with its own locking rules in crawler/frontier.py.
import sqlite3
import threading
from contextlib import contextmanager

DATABASE_LOCK = threading.RLock()


@contextmanager
def write_database(path):
    # Connections belong to their calling thread. BEGIN IMMEDIATE also makes
    # similarity lookup + insertion one atomic transaction in SQLite itself.
    with DATABASE_LOCK:
        # BEGIN IMMEDIATE reserves the write transaction before a read followed
        # by an insert. Two workers cannot both miss the same exact-text record
        # and then race to classify themselves as the first occurrence.
        connection = sqlite3.connect(path, timeout=30)
        try:
            with connection:
                connection.execute("BEGIN IMMEDIATE")
                yield connection
        finally:
            # Connection context managers commit/rollback but do not close the
            # connection. Explicit closing also runs when processing raises.
            connection.close()
