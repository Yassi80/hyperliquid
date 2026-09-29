"""Shared REST weight budget.

Hyperliquid limits REST info requests to a weight of 1200 per minute per IP. Every
hlflows process (recorder, backfill, CLI) records what it spends in the SQLite
`weight_ledger`, and reserves weight against a sliding 60-second window before each
request, so processes running side by side stay under the configured ceiling together.
"""

from __future__ import annotations

import asyncio
import os
import random
import sqlite3
from collections.abc import Awaitable, Callable, Iterator

from hlflows.timeutil import MINUTE_MS, now_ms

WINDOW_MS = MINUTE_MS


class WeightBudget:
    def __init__(
        self,
        conn: sqlite3.Connection,
        ceiling: int,
        clock: Callable[[], int] = now_ms,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self.conn = conn
        self.ceiling = ceiling
        self.clock = clock
        self.sleep = sleep
        self.pid = os.getpid()

    def used(self) -> int:
        now = self.clock()
        row = self.conn.execute(
            "SELECT COALESCE(SUM(weight), 0) FROM weight_ledger WHERE ts > ?", (now - WINDOW_MS,)
        ).fetchone()
        return int(row[0])

    def try_reserve(self, weight: int, what: str) -> int:
        """Reserve `weight` now if it fits. Returns 0 on success, else ms to wait."""
        now = self.clock()
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            self.conn.execute("DELETE FROM weight_ledger WHERE ts <= ?", (now - 2 * WINDOW_MS,))
            rows = self.conn.execute(
                "SELECT ts, weight FROM weight_ledger WHERE ts > ? ORDER BY ts",
                (now - WINDOW_MS,),
            ).fetchall()
            used = sum(r["weight"] for r in rows)
            # A single request heavier than the ceiling may run once the window is empty.
            if used + weight <= self.ceiling or used == 0:
                self._insert(now, weight, what)
                self.conn.execute("COMMIT")
                return 0
            self.conn.execute("COMMIT")
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise
        # Wait until enough old entries leave the window.
        need = used + weight - self.ceiling
        freed = 0
        for r in rows:
            freed += r["weight"]
            if freed >= need:
                return max(1, int(r["ts"]) + WINDOW_MS - now + 1)
        return WINDOW_MS

    async def acquire(self, weight: int, what: str) -> None:
        while (wait_ms := self.try_reserve(weight, what)) > 0:
            await self.sleep(wait_ms / 1000)

    def charge(self, weight: int, what: str) -> None:
        """Record weight known only after the response (per-item surcharges)."""
        if weight > 0:
            self._insert(self.clock(), weight, what)

    def _insert(self, ts: int, weight: int, what: str) -> None:
        self.conn.execute(
            "INSERT INTO weight_ledger (ts, weight, pid, what) VALUES (?, ?, ?, ?)",
            (ts, weight, self.pid, what),
        )


def backoff_delays(base_s: float = 0.5, cap_s: float = 30.0) -> Iterator[float]:
    """Exponential backoff with full jitter: uniform(0, min(cap, base * 2**attempt))."""
    attempt = 0
    while True:
        yield random.uniform(0, min(cap_s, base_s * 2**attempt))
        attempt += 1
