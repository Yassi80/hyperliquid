from __future__ import annotations

from datetime import UTC, datetime

import pytest

from hlflows.timeutil import (
    TIMEFRAMES,
    bucket_start,
    from_ms,
    next_bucket_start,
    parse_timeframes,
    to_ms,
)


def ms(y: int, mo: int, d: int, h: int = 0, mi: int = 0, s: int = 0) -> int:
    return to_ms(datetime(y, mo, d, h, mi, s, tzinfo=UTC))


def test_intraday_alignment_is_epoch_based() -> None:
    t = ms(2026, 9, 23, 13, 47, 12)
    assert bucket_start(t, TIMEFRAMES["15m"]) == ms(2026, 9, 23, 13, 45)
    assert bucket_start(t, TIMEFRAMES["1h"]) == ms(2026, 9, 23, 13)
    assert bucket_start(t, TIMEFRAMES["4h"]) == ms(2026, 9, 23, 12)
    assert bucket_start(t, TIMEFRAMES["1d"]) == ms(2026, 9, 23)


def test_weekly_bars_open_monday_utc() -> None:
    # 2026-09-23 is a Wednesday; its week opens Monday 2026-09-21 00:00 UTC.
    start = bucket_start(ms(2026, 9, 23, 13), TIMEFRAMES["1w"])
    assert start == ms(2026, 9, 21)
    assert from_ms(start).weekday() == 0
    # Monday midnight belongs to the new week; one ms earlier to the previous one.
    assert bucket_start(ms(2026, 9, 21), TIMEFRAMES["1w"]) == ms(2026, 9, 21)
    assert bucket_start(ms(2026, 9, 21) - 1, TIMEFRAMES["1w"]) == ms(2026, 9, 14)


def test_weekly_anchor_is_configurable() -> None:
    assert bucket_start(ms(2026, 9, 23, 13), TIMEFRAMES["1w"], "sunday") == ms(2026, 9, 20)


def test_monthly_bars() -> None:
    assert bucket_start(ms(2026, 2, 28, 23, 59), TIMEFRAMES["1M"]) == ms(2026, 2, 1)
    assert next_bucket_start(ms(2026, 12, 1), TIMEFRAMES["1M"]) == ms(2027, 1, 1)
    assert next_bucket_start(ms(2026, 2, 1), TIMEFRAMES["1M"]) == ms(2026, 3, 1)


def test_parse_timeframes() -> None:
    assert [t.name for t in parse_timeframes("1h, 4h,1d")] == ["1h", "4h", "1d"]
    with pytest.raises(ValueError):
        parse_timeframes("2h")
