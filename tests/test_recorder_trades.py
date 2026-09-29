from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest

from hlflows.api import types as t
from hlflows.recorder.live import LiveWindow, ceil_minute
from hlflows.recorder.trades import (
    aggregate_minute,
    build_bar,
    flush_raw_trades,
    hour_path,
    insert_trades,
    read_raw_trades,
)
from hlflows.timeutil import HOUR_MS, MINUTE_MS, to_ms

T0 = to_ms(datetime(2026, 8, 3, 10, tzinfo=UTC))
BY_HL = {"BTC": "BTC", "@142": "BTC/spot"}


def trade(tid: int, ts: int, side: str, px: float, sz: float, coin: str = "BTC") -> t.WsTrade:
    return t.WsTrade(coin, side, px, sz, ts, f"0x{tid:x}", tid, ("0xbuyer", "0xseller"))


def test_aggregate_minute() -> None:
    bar = aggregate_minute("BTC", T0, [(100, 1, "B"), (102, 2, "A"), (99, 0.5, "B"), (101, 1, "A")],
                           partial=False)  # fmt: skip
    assert bar is not None
    assert (bar.o, bar.h, bar.l, bar.c) == (100, 102, 99, 101)
    assert bar.volume == 4.5 and bar.n == 4
    assert bar.buy_sz == 1.5 and bar.sell_sz == 3
    assert bar.buy_ntl == pytest.approx(100 + 49.5)
    assert bar.sell_ntl == pytest.approx(204 + 101)
    assert bar.source == "ws" and bar.quality is None
    partial = aggregate_minute("BTC", T0, [(1, 1, "B")], partial=True, low_significance=True)
    assert partial is not None and partial.quality == "partial,low_significance"
    assert aggregate_minute("BTC", T0, [], partial=False) is None


def test_replayed_trades_are_not_double_counted(conn: sqlite3.Connection) -> None:
    batch = [trade(i, T0 + i * 1000, "B" if i % 2 else "A", 100 + i, 1) for i in range(10)]
    inserted, touched = insert_trades(conn, BY_HL, batch)
    assert [tr.tid for _, tr in inserted] == list(range(10)) and touched == {("BTC", T0)}
    inserted, touched = insert_trades(conn, BY_HL, batch[5:])  # replay on resubscribe
    assert inserted == [] and touched == set()
    bar = build_bar(conn, "BTC", T0, partial=False, low_significance=False)
    assert bar is not None and bar.n == 10 and bar.buy_sz == 5


def test_unknown_coins_are_ignored(conn: sqlite3.Connection) -> None:
    inserted, _ = insert_trades(conn, BY_HL, [trade(1, T0, "B", 1, 1, coin="DOGE")])
    assert inserted == []


def test_flush_raw_trades(conn: sqlite3.Connection, tmp_path: Path) -> None:
    trades = [trade(i, T0 + i * 60_000, "B", 100, 1) for i in range(70)]  # 10:00 → 11:09
    trades.append(trade(999, T0 + 5 * 1000, "A", 1, 1, coin="@142"))
    insert_trades(conn, BY_HL, trades)
    written = flush_raw_trades(conn, tmp_path, T0 + HOUR_MS + 30 * MINUTE_MS)
    assert sorted(written) == [("BTC", T0, 60), ("BTC/spot", T0, 1)]
    path = hour_path(tmp_path, "BTC", T0)
    assert path == tmp_path / "market=BTC" / "date=2026-08-03" / "hour=10.parquet"
    assert (tmp_path / "market=BTC_spot" / "date=2026-08-03" / "hour=10.parquet").exists()
    # The unfinished hour stays staged.
    assert conn.execute("SELECT COUNT(*) FROM trades_staging").fetchone()[0] == 10

    # A crash after writing but before unstaging, or a late trade: merge, dedupe by tid.
    insert_trades(conn, BY_HL, [trades[3], trade(500, T0 + 59 * MINUTE_MS + 1, "A", 1, 2)])
    flush_raw_trades(conn, tmp_path, T0 + HOUR_MS + 30 * MINUTE_MS)
    df = read_raw_trades(tmp_path, "BTC", T0, T0 + HOUR_MS)
    assert df.height == 61
    assert df["tid"].n_unique() == 61
    assert df["ts"].is_sorted()


def test_live_window() -> None:
    w = LiveWindow()
    assert w.on_trade(T0) is None  # not subscribed
    live_from = w.on_ack(T0 + 20_000)
    assert live_from == T0 + MINUTE_MS == ceil_minute(T0 + 20_000)
    assert w.on_trade(T0 + 10_000) is None  # replayed trade from before the ack
    assert w.on_trade(T0 + 70_000) is None  # inside the first live minute
    assert w.on_trade(T0 + 3 * MINUTE_MS + 5) == (T0 + MINUTE_MS, T0 + 3 * MINUTE_MS)
    assert w.is_full(T0 + MINUTE_MS) and not w.is_full(T0)
    w.on_disconnect(T0 + 4 * MINUTE_MS + 30_000)
    assert w.is_full(T0 + 3 * MINUTE_MS)
    assert not w.is_full(T0 + 4 * MINUTE_MS)  # the disconnect happened inside it
    w.on_ack(T0 + 6 * MINUTE_MS)
    assert w.is_full(T0 + 6 * MINUTE_MS) and not w.is_full(T0 + 5 * MINUTE_MS)
