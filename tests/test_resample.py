from __future__ import annotations

import sqlite3
from datetime import UTC, datetime

import pytest

from hlflows import coverage as cov
from hlflows import data, store
from hlflows.markets import Market, save_markets
from hlflows.timeutil import DAY_MS, HOUR_MS, MINUTE_MS, TIMEFRAMES, to_ms

T0 = to_ms(datetime(2026, 8, 3, tzinfo=UTC))  # a Monday, well in the past


def add_bars(
    conn: sqlite3.Connection,
    res: str,
    start: int,
    count: int,
    market: str = "BTC",
    source: str = "ws",
    taker: bool = True,
) -> None:
    step = data.STORED_RES[res]
    bars = []
    for i in range(count):
        px = 100.0 + i
        bars.append(
            store.Bar(
                market,
                res,
                start + i * step,
                px,
                px + 0.5,
                px - 0.5,
                px + 0.25,
                1.0,
                1,
                0.6 if taker else None,
                0.4 if taker else None,
                60.0 if taker else None,
                40.0 if taker else None,
                source,
                None,
            )
        )
    store.upsert_bars(conn, bars)
    cov.add_coverage(conn, data.bars_stream(res), market, start, start + count * step)
    if taker and res == "1m":
        cov.add_coverage(conn, data.TAKER_STREAM, market, start, start + count * step)


def test_1m_to_1h(conn: sqlite3.Connection) -> None:
    add_bars(conn, "1m", T0, 120)
    df = data.load_bars(conn, "BTC", TIMEFRAMES["1h"], T0, T0 + 2 * HOUR_MS)
    assert df["ts"].to_list() == [T0, T0 + HOUR_MS]
    first = df.row(0, named=True)
    assert first["o"] == 100.0
    assert first["c"] == pytest.approx(159.25)
    assert first["h"] == pytest.approx(159.5)
    assert first["l"] == pytest.approx(99.5)
    assert first["volume"] == pytest.approx(60.0)
    assert first["n"] == 60
    assert first["delta"] == pytest.approx(60 * 0.2)
    assert first["res"] == "1m" and first["complete"] and first["taker"] and first["closed"]


def test_4h_bars_align_to_utc_and_flag_partial(conn: sqlite3.Connection) -> None:
    add_bars(conn, "1m", T0 + 2 * HOUR_MS, 6 * 60)  # 02:00 → 08:00
    df = data.load_bars(conn, "BTC", TIMEFRAMES["4h"], T0, T0 + 8 * HOUR_MS)
    assert df["ts"].to_list() == [T0, T0 + 4 * HOUR_MS]
    assert df["complete"].to_list() == [False, True]
    assert df["quality"].to_list() == ["partial,taker_partial", None]
    assert df["taker_cov"].to_list() == [pytest.approx(0.5), 1.0]


def test_falls_back_to_hourly_candles_keeping_partial_delta(conn: sqlite3.Connection) -> None:
    add_bars(conn, "1h", T0, 24, source="candle", taker=False)
    add_bars(conn, "1m", T0 + 60 * MINUTE_MS, 30)  # 30 recorded minutes inside one 4h bar
    df = data.load_bars(conn, "BTC", TIMEFRAMES["4h"], T0, T0 + DAY_MS)
    assert df.height == 6
    assert set(df["res"].to_list()) == {"1h"}  # OHLCV from the complete hourly candles
    assert df["complete"].all()
    # The taker split comes from the recorded minutes, labelled as partial.
    first = df.row(0, named=True)
    assert first["delta"] == pytest.approx(30 * 0.2)
    assert first["taker_cov"] == pytest.approx(30 / 240)
    assert first["quality"] == "taker_partial" and not first["taker"]
    assert df["delta"].null_count() == 5


def test_reconnect_minute_does_not_blank_the_bar(conn: sqlite3.Connection) -> None:
    """A bar missing one minute (a reconnect) keeps its delta, flagged partial."""
    add_bars(conn, "1m", T0, 30)
    add_bars(conn, "1m", T0 + 31 * MINUTE_MS, 29)  # minute 30 was lost
    df = data.load_bars(conn, "BTC", TIMEFRAMES["1h"], T0, T0 + HOUR_MS)
    row = df.row(0, named=True)
    assert row["delta"] == pytest.approx(59 * 0.2)
    assert row["taker_cov"] == pytest.approx(59 / 60)
    assert "taker_partial" in row["quality"] and not row["taker"]


def test_prefers_finest_complete_resolution(conn: sqlite3.Connection) -> None:
    add_bars(conn, "1h", T0, 24, source="candle", taker=False)
    add_bars(conn, "1m", T0, 4 * 60)  # 1m fully covers the first 4h bar
    df = data.load_bars(conn, "BTC", TIMEFRAMES["4h"], T0, T0 + DAY_MS)
    assert df["res"].to_list() == ["1m", "1h", "1h", "1h", "1h", "1h"]
    assert df["taker"].to_list() == [True, False, False, False, False, False]


def test_weekly_from_daily_opens_monday(conn: sqlite3.Connection) -> None:
    add_bars(conn, "1d", T0 - 2 * DAY_MS, 16, source="candle", taker=False)  # Sat → Sun+2w
    df = data.load_bars(conn, "BTC", TIMEFRAMES["1w"], T0, T0 + 14 * DAY_MS)
    assert df["ts"].to_list() == [T0, T0 + 7 * DAY_MS]
    assert df["volume"].to_list() == [7.0, 7.0]
    assert df["o"][0] == 102.0  # Monday's open, not Saturday's
    sunday = data.load_bars(conn, "BTC", TIMEFRAMES["1w"], T0, T0 + 7 * DAY_MS, "sunday")
    assert sunday["ts"][0] == T0 - DAY_MS


def test_monthly_from_daily(conn: sqlite3.Connection) -> None:
    aug1 = to_ms(datetime(2026, 8, 1, tzinfo=UTC))
    add_bars(conn, "1d", aug1, 34, source="candle", taker=False)  # Aug 1 → Sep 3
    df = data.load_bars(conn, "BTC", TIMEFRAMES["1M"], aug1, aug1 + 34 * DAY_MS)
    assert df["ts"].to_list() == [aug1, to_ms(datetime(2026, 9, 1, tzinfo=UTC))]
    assert df["volume"].to_list() == [31.0, 3.0]
    assert df["complete"].to_list() == [True, False]


def test_low_significance_spot_is_labelled(conn: sqlite3.Connection) -> None:
    save_markets(conn, [Market("BTC/spot", "BTC", "spot", "@142", "low")])
    add_bars(conn, "1m", T0, 60, market="BTC/spot")
    df = data.load_bars(conn, "BTC/spot", TIMEFRAMES["1h"], T0, T0 + HOUR_MS)
    assert df["quality"][0] == "low_significance"


def test_funding_bars(conn: sqlite3.Connection) -> None:
    from hlflows.api import types as t

    recs = [
        t.FundingRecord("BTC", 0.00001 * (i + 1), 0.0001, T0 + i * HOUR_MS + 17) for i in range(8)
    ]
    store.upsert_funding(conn, recs)
    four = data.load_funding_bars(conn, "BTC", TIMEFRAMES["4h"], T0, T0 + 8 * HOUR_MS)
    assert four["ts"].to_list() == [T0, T0 + 4 * HOUR_MS]
    assert four["rate_1h"][0] == pytest.approx(0.000025)
    assert four["rate_8h"][0] == pytest.approx(0.0002)
    assert four["hours"].to_list() == [4, 4]
    q = data.load_funding_bars(conn, "BTC", TIMEFRAMES["15m"], T0, T0 + HOUR_MS)
    assert q.height == 4 and q["rate_1h"].to_list() == [pytest.approx(0.00001)] * 4
