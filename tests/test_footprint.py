from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pytest

from hlflows import footprint as fp
from hlflows.api import types as t
from hlflows.config import Config, FootprintConfig
from hlflows.recorder.trades import flush_raw_trades, insert_trades
from hlflows.timeutil import HOUR_MS, MINUTE_MS, to_ms

T0 = to_ms(datetime(2026, 8, 3, 10, tzinfo=UTC))
BY_HL = {"BTC": "BTC", "@142": "BTC/spot"}
TICKS = {"BTC": 5.0, "BTC/spot": 5.0}


def trade(tid: int, ts: int, side: str, px: float, sz: float, coin: str = "BTC") -> t.WsTrade:
    return t.WsTrade(coin, side, px, sz, ts, f"0x{tid:x}", tid, ("0xbuyer", "0xseller"))


def frame(rows: list[tuple[int, str, float, float]]) -> pl.DataFrame:
    return pl.DataFrame(rows, schema=["ts", "side", "px", "sz"], orient="row")


def stored(conn: sqlite3.Connection, market: str = "BTC") -> list[tuple[int, int, float, float]]:
    return [tuple(r) for r in conn.execute(
        "SELECT ts, lvl, buy_sz, sell_sz FROM footprint WHERE market = ? ORDER BY ts, lvl",
        (market,),
    )]  # fmt: skip


def test_levels_split_taker_sides_per_15m_and_price_step() -> None:
    df = frame([
        (T0, "B", 100.0, 1.0), (T0 + 1, "A", 104.99, 2.0), (T0 + 2, "B", 105.0, 0.5),
        (T0 + 15 * MINUTE_MS, "A", 100.0, 3.0),
    ])  # fmt: skip
    rows = fp.levels(df, 5.0).rows()
    assert rows == [(T0, 20, 1.0, 2.0), (T0, 21, 0.5, 0.0), (T0 + 15 * MINUTE_MS, 20, 0.0, 3.0)]
    # px / tick a hair below a whole level in floating point still lands on that level
    assert fp.levels(frame([(T0, "B", 90.26, 1.0)]), 0.02)["lvl"].to_list() == [4513]


def test_flush_stores_the_footprint_and_rewrites_it_with_late_trades(
    conn: sqlite3.Connection, tmp_path: Path
) -> None:
    trades = [trade(i, T0 + i * MINUTE_MS, "B", 100 + i, 1) for i in range(70)]  # 10:00 → 11:09
    trades.append(trade(999, T0 + 5_000, "A", 102, 2, coin="@142"))
    insert_trades(conn, BY_HL, trades)
    flush_raw_trades(conn, tmp_path, T0 + HOUR_MS + 30 * MINUTE_MS, TICKS)
    rows = stored(conn)
    assert sum(r[2] for r in rows) == 60  # the finished hour only; 11:00+ is still staged
    assert rows[0] == (T0, 20, 5.0, 0.0)  # 100..104 in the first 15 minutes
    assert stored(conn, "BTC/spot") == [(T0, 20, 0.0, 2.0)]

    insert_trades(conn, BY_HL, [trade(500, T0 + 59 * MINUTE_MS, "A", 101, 4)])  # late
    flush_raw_trades(conn, tmp_path, T0 + HOUR_MS + 30 * MINUTE_MS, TICKS)
    rows = stored(conn)
    assert sum(r[2] for r in rows) == 60 and sum(r[3] for r in rows) == 4

    # Without ticks (or for markets without one) nothing is stored.
    insert_trades(conn, BY_HL, [trade(600, T0 + 2 * HOUR_MS, "B", 100, 1)])
    flush_raw_trades(conn, tmp_path, T0 + 3 * HOUR_MS)
    assert conn.execute("SELECT MAX(ts) FROM footprint").fetchone()[0] < T0 + HOUR_MS


def test_load_adds_trades_still_staged(conn: sqlite3.Connection, tmp_path: Path) -> None:
    insert_trades(conn, BY_HL, [trade(1, T0, "B", 100, 1), trade(2, T0 + HOUR_MS, "A", 100, 2)])
    flush_raw_trades(conn, tmp_path, T0 + HOUR_MS + 5 * MINUTE_MS, TICKS)
    insert_trades(conn, BY_HL, [trade(3, T0 + 1, "A", 101, 7)])  # late, not merged yet
    df = fp.load(conn, "BTC", T0, T0 + 2 * HOUR_MS, 5.0).sort("ts", "buy_sz")
    assert df.rows() == [(T0, 100.0, 0.0, 7.0), (T0, 100.0, 1.0, 0.0),
                         (T0 + HOUR_MS, 100.0, 0.0, 2.0)]  # fmt: skip


def test_pick_step_is_a_whole_multiple_of_the_tick() -> None:
    assert fp.pick_step(5.0, [170, 180, 400], 24) == 10.0  # 180 / 24 = 7.5
    assert fp.pick_step(5.0, [600], 24) == 25.0
    assert fp.pick_step(0.02, [0.4], 24) == pytest.approx(0.02)
    assert fp.pick_step(0.02, [0.6], 24) == pytest.approx(0.04)  # 0.025 → 2 ticks
    assert fp.pick_step(5.0, [], 24) == 5.0


def test_series_groups_rows_into_bars_and_levels() -> None:
    rows = pl.DataFrame(
        [(T0, 100.0, 1.0, 0.0), (T0, 105.0, 0.0, 2.0), (T0 + 15 * MINUTE_MS, 110.0, 3.0, 1.0),
         (T0 + 2 * HOUR_MS, 95.0, 1.0, 1.0)],
        schema=["ts", "px", "buy_sz", "sell_sz"], orient="row",
    )  # fmt: skip
    bars = [T0, T0 + HOUR_MS, T0 + 2 * HOUR_MS]
    s = fp.series(rows, bars, HOUR_MS, [15.0], 5.0, 3)
    assert s is not None and s["step"] == 5.0
    assert s["lo"] == [20, None, 19]
    assert s["buy"] == [[1.0, 0.0, 3.0], None, [1.0]]
    assert s["sell"] == [[0.0, 2.0, 1.0], None, [1.0]]
    assert s["poc"] == [112.5, None, 97.5]  # busiest level's middle
    assert fp.series(rows.clear(), bars, HOUR_MS, [15.0], 5.0, 3) is None


def test_rebuild_from_parquet_matches_the_recorder(
    conn: sqlite3.Connection, tmp_path: Path
) -> None:
    trades = [trade(i, T0 + i * 30_000, "AB"[i % 2], 100 + i % 13, 0.5) for i in range(240)]
    insert_trades(conn, BY_HL, trades)
    flush_raw_trades(conn, tmp_path, T0 + 3 * HOUR_MS, TICKS)
    before = stored(conn)
    conn.execute("DELETE FROM footprint")
    cfg = FootprintConfig(tick={"BTC": 5.0})
    assert fp.rebuild(conn, tmp_path, cfg, T0 + HOUR_MS) == 1  # only hours since `since`
    assert stored(conn) == [r for r in before if r[0] >= T0 + HOUR_MS]
    assert fp.rebuild(conn, tmp_path, cfg, 0) == 2
    assert stored(conn) == before
    assert fp.prune(conn, T0 + HOUR_MS) == len([r for r in before if r[0] < T0 + HOUR_MS])
    assert stored(conn)[0][0] == T0 + HOUR_MS


def test_footprint_config_is_validated() -> None:
    cfg = Config(footprint=FootprintConfig(timeframes=["1w"]))
    with pytest.raises(ValueError, match=r"footprint\.timeframes"):
        cfg.validate()
    Config(footprint=FootprintConfig(timeframes=["15m", "4h"])).validate()
