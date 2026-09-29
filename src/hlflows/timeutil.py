"""UTC millisecond timestamps and timeframe definitions.

Every timestamp in hlflows is an int of milliseconds since the Unix epoch, UTC.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import UTC, datetime

MINUTE_MS = 60_000
HOUR_MS = 60 * MINUTE_MS
DAY_MS = 24 * HOUR_MS
WEEK_MS = 7 * DAY_MS

WEEKDAYS = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def to_ms(dt: datetime) -> int:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return int(dt.timestamp() * 1000)


def from_ms(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=UTC)


def fmt_ms(ms: int) -> str:
    return from_ms(ms).strftime("%Y-%m-%d %H:%M")


def parse_date(text: str) -> int:
    """Parse `YYYY-MM-DD` or an ISO datetime (assumed UTC if naive) to ms."""
    return to_ms(datetime.fromisoformat(text))


def floor_ms(ms: int, step_ms: int) -> int:
    return ms - ms % step_ms


@dataclass(frozen=True)
class Timeframe:
    """A resampling timeframe.

    `fixed_ms` is set for timeframes with a constant length (up to 1w).
    Months have variable length, so `fixed_ms` is None for `1M`.
    `polars_every` is the matching `group_by_dynamic` interval.
    """

    name: str
    fixed_ms: int | None
    polars_every: str


TIMEFRAMES: dict[str, Timeframe] = {
    "1m": Timeframe("1m", MINUTE_MS, "1m"),
    "5m": Timeframe("5m", 5 * MINUTE_MS, "5m"),
    "15m": Timeframe("15m", 15 * MINUTE_MS, "15m"),
    "1h": Timeframe("1h", HOUR_MS, "1h"),
    "4h": Timeframe("4h", 4 * HOUR_MS, "4h"),
    "1d": Timeframe("1d", DAY_MS, "1d"),
    "1w": Timeframe("1w", WEEK_MS, "1w"),
    "1M": Timeframe("1M", None, "1mo"),
}


def parse_timeframes(text: str) -> list[Timeframe]:
    out = []
    for part in text.split(","):
        name = part.strip()
        if name not in TIMEFRAMES:
            raise ValueError(f"unknown timeframe {name!r}; choose from {', '.join(TIMEFRAMES)}")
        out.append(TIMEFRAMES[name])
    return out


def bucket_start(ms: int, tf: Timeframe, week_start: str = "monday") -> int:
    """Start of the bar containing `ms`, in UTC.

    Intraday and daily bars align to the epoch (so 4h bars open at 00, 04, 08 UTC...).
    Weekly bars open at 00:00 UTC on `week_start`; monthly bars on the 1st.
    """
    if tf.name == "1M":
        dt = from_ms(ms)
        return to_ms(datetime(dt.year, dt.month, 1, tzinfo=UTC))
    assert tf.fixed_ms is not None
    if tf.name == "1w":
        # 1970-01-01 was a Thursday (weekday index 3).
        offset = ((WEEKDAYS.index(week_start) - 3) % 7) * DAY_MS
        return floor_ms(ms - offset, WEEK_MS) + offset
    return floor_ms(ms, tf.fixed_ms)


def next_bucket_start(start: int, tf: Timeframe) -> int:
    if tf.name == "1M":
        dt = from_ms(start)
        year, month = (dt.year + 1, 1) if dt.month == 12 else (dt.year, dt.month + 1)
        return to_ms(datetime(year, month, 1, tzinfo=UTC))
    assert tf.fixed_ms is not None
    return start + tf.fixed_ms
