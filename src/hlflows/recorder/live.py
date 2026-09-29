"""Which minutes a live subscription is authoritative for.

A market's stream is live from the first whole minute after its subscription is
acknowledged. Coverage is only *confirmed* up to the minute of the latest trade seen:
a trade stamped in minute M proves the stream delivered everything before M. So if a
subscription dies silently, the unconfirmed tail never becomes coverage, and the
reconnect records it as a gap.
"""

from __future__ import annotations

from hlflows.timeutil import MINUTE_MS, floor_ms


def ceil_minute(ms: int) -> int:
    return -(-ms // MINUTE_MS) * MINUTE_MS


class LiveWindow:
    def __init__(self) -> None:
        self.live_from: int | None = None
        self.confirmed_to: int | None = None
        # Recent live intervals [from, until); until is None while connected.
        self.intervals: list[tuple[int, int | None]] = []

    @property
    def connected(self) -> bool:
        return self.live_from is not None

    def on_ack(self, now: int) -> int:
        """Subscription acknowledged at `now`. Returns the first fully live minute."""
        self.live_from = ceil_minute(now)
        self.confirmed_to = self.live_from
        self.intervals = [*self.intervals[-9:], (self.live_from, None)]
        return self.live_from

    def on_trade(self, ts: int) -> tuple[int, int] | None:
        """A trade stamped `ts` arrived. Returns a newly confirmed [start, end) or None."""
        if self.live_from is None or self.confirmed_to is None:
            return None
        minute = floor_ms(ts, MINUTE_MS)
        if minute <= self.confirmed_to:
            return None
        span = (self.confirmed_to, minute)
        self.confirmed_to = minute
        return span

    def on_disconnect(self, now: int) -> None:
        if self.intervals and self.intervals[-1][1] is None:
            self.intervals[-1] = (self.intervals[-1][0], now)
        self.live_from = None
        self.confirmed_to = None

    def is_full(self, minute: int) -> bool:
        """Whether the stream was live for the whole of `minute`."""
        end = minute + MINUTE_MS
        return any(
            start <= minute and (until is None or end <= until) for start, until in self.intervals
        )
