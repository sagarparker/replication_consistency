"""A small, in-memory primary/secondary replication simulation (stdlib only)."""

from collections import deque
from dataclasses import dataclass
from threading import Condition, Thread
from time import monotonic
from typing import Literal


def _validate_stock(stock: int) -> None:
    if not isinstance(stock, int) or isinstance(stock, bool):
        raise TypeError("Stock must be an integer")
    if stock < 0:
        raise ValueError("Stock must be nonnegative")


@dataclass(frozen=True)
class Inventory:
    stock: int
    version: int


class ReplicatedInventory:
    """Accept primary writes and asynchronously apply them to one secondary.

    The condition protects state and makes pause_replication a barrier: after it
    returns, the secondary cannot change until resume_replication is called.
    Versions make it possible to wait for a particular write, including when
    multiple writes happen to contain the same stock value.
    """

    def __init__(self, stock: int = 10, replication_delay: float = 0.1):
        _validate_stock(stock)
        if replication_delay < 0:
            raise ValueError("Replication delay must be nonnegative")
        self._primary = Inventory(stock, 0)
        self._secondary = Inventory(stock, 0)
        self._pending: deque[Inventory] = deque()
        self._condition = Condition()
        self._paused = False
        self._closed = False
        self._delay = replication_delay
        self._worker = Thread(target=self._replicate, name="replicator", daemon=True)
        self._worker.start()

    def read(self, replica: Literal["primary", "secondary"]) -> Inventory:
        """Client-selectable reads; secondary reads never wait for replication."""
        with self._condition:
            if replica == "primary":
                return self._primary
            if replica == "secondary":
                return self._secondary
            raise ValueError("Replica must be 'primary' or 'secondary'")

    def write(self, stock: int) -> Inventory:
        """Acknowledge a primary write immediately and enqueue it for replication."""
        _validate_stock(stock)
        with self._condition:
            if self._closed:
                raise RuntimeError("System is closed")
            self._primary = Inventory(stock, self._primary.version + 1)
            self._pending.append(self._primary)
            self._condition.notify_all()
            return self._primary

    def pause_replication(self) -> None:
        with self._condition:
            self._paused = True
            self._condition.notify_all()

    def resume_replication(self) -> None:
        with self._condition:
            self._paused = False
            self._condition.notify_all()

    def wait_for_replication(self, version: int, timeout: float = 2.0) -> None:
        """Wait for a version; fail if shutdown prevents it from being applied."""
        with self._condition:
            ready = self._condition.wait_for(
                lambda: self._secondary.version >= version or self._closed,
                timeout=timeout,
            )
            if self._secondary.version < version and self._closed:
                raise RuntimeError("System is closed before replication completed")
            if not ready:
                raise TimeoutError(f"Secondary did not reach version {version}")

    def _replicate(self) -> None:
        with self._condition:
            while True:
                self._condition.wait_for(
                    lambda: self._closed or (self._pending and not self._paused)
                )
                if self._closed:
                    return
                deadline = monotonic() + self._delay
                while not self._closed and not self._paused:
                    remaining = deadline - monotonic()
                    if remaining <= 0:
                        break
                    self._condition.wait(remaining)
                if self._closed:
                    return
                if self._paused:
                    continue
                self._secondary = self._pending.popleft()
                self._condition.notify_all()

    def close(self) -> None:
        """Stop the worker; this simulation does not persist pending writes."""
        with self._condition:
            self._closed = True
            self._condition.notify_all()
        self._worker.join()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
