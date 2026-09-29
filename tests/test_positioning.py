from __future__ import annotations

import sqlite3

import polars as pl
import pytest

from hlflows import store
from hlflows.config import WalletsConfig
from hlflows.wallets import positioning as pos
from hlflows.wallets import store as ws


def frame(rows: list[tuple[float, float, float | None, float | None]]) -> pl.DataFrame:
    """(szi, position_value, liq_px, eff_lev) rows."""
    return pl.DataFrame(
        [(f"0x{i}", szi, val, liq, 10.0, lev, "cross", 1e6) for i, (szi, val, liq, lev)
         in enumerate(rows)],
        schema=pos.POSITION_SCHEMA, orient="row",
    )  # fmt: skip


def test_liqmap_buckets_and_cumulates_outward() -> None:
    df = frame([
        (1, 100, 97.5, 5), (2, 200, 99.5, 5), (1, 50, 80.0, 5),   # longs; 80 is out of range
        (-1, 300, 101.2, 5), (-1, 400, 104.9, 5), (-1, 999, None, 1),  # shorts; one without liq
    ])  # fmt: skip
    m = pos.liqmap(df, mark=100.0, bucket_pct=1.0, range_pct=5.0)
    by_k = {r["k"]: r for r in m.iter_rows(named=True)}
    assert set(by_k) == set(range(-5, 5))
    assert by_k[-3]["long_ntl"] == 100 and by_k[-1]["long_ntl"] == 200
    assert by_k[1]["short_ntl"] == 300 and by_k[4]["short_ntl"] == 400
    assert by_k[-3]["px_lo"] == pytest.approx(97.0) and by_k[-3]["px_hi"] == pytest.approx(98.0)
    # Cumulative from the mark outward.
    assert by_k[-1]["cum_long"] == 200 and by_k[-3]["cum_long"] == 300
    assert by_k[-5]["cum_long"] == 300  # the 80 liquidation is outside +-5%
    assert by_k[0]["cum_short"] == 0 and by_k[1]["cum_short"] == 300
    assert by_k[4]["cum_short"] == 700
    assert m["k"].to_list() == sorted(m["k"].to_list())


def test_summary_coverage_counts_one_side_of_oi() -> None:
    df = frame([(1, 3e6, 90, 4), (1, 1e6, 80, 2), (-1, 1e6, 110, 10)])
    ref = pos.MarketRef(ts=0, oi=200.0, mark=1e5)  # OI notional 20M
    s = pos.summarize(df, 0, ref)
    assert s.long_ntl == 4e6 and s.short_ntl == 1e6 and s.net_ntl == 3e6
    assert s.long_cov == pytest.approx(0.2)
    assert s.short_cov == pytest.approx(0.05)
    assert s.combined_cov == pytest.approx(5e6 / 40e6)
    assert s.lev_long == pytest.approx((3e6 * 4 + 1e6 * 2) / 4e6)
    assert pos.summarize(df, 0, None).long_cov is None


def test_leverage_distribution() -> None:
    df = frame([(1, 100, 90, 1.5), (1, 200, 90, 7), (-1, 300, 110, 25), (-1, 5, 110, None)])
    d = {r["bucket"]: r for r in pos.leverage_distribution(df).iter_rows(named=True)}
    assert d["0-2x"]["long_ntl"] == 100 and d["5-10x"]["long_ntl"] == 200
    assert d["20x+"]["short_ntl"] == 300 and d["20x+"]["n_short"] == 1
    assert sum(r["n_short"] for r in d.values()) == 1  # unknown leverage not bucketed


def _positions(conn: sqlite3.Connection, round_ts: int, rows: list[tuple[str, float]]) -> None:
    ws.insert_positions(conn, [
        ws.PositionRow(round_ts, a, "BTC", szi, 1.0, abs(szi) * 100, "cross", 10, 5, 90, 0, 1, 1e4)
        for a, szi in rows
    ])  # fmt: skip
    conn.execute("INSERT INTO snapshot_rounds VALUES (?, ?, 0, 1)", (round_ts, len(rows)))


def test_excluded_wallets_and_same_wallet_net_change(conn: sqlite3.Connection) -> None:
    w = WalletsConfig()
    with store.transaction(conn):
        ws.set_wallet(conn, "0xa", "active", "t", 0)
        ws.set_wallet(conn, "0xc", "active", "t", 0)
        ws.set_wallet(conn, "0xv", "active", "t", 0, tags="vault")
        ws.set_wallet(conn, "0xb", "active", "t", 150)  # joins between the rounds
        _positions(conn, 100, [("0xa", 1), ("0xc", 5), ("0xv", 50)])
        _positions(conn, 200, [("0xa", 3), ("0xb", 10), ("0xv", -50)])
        conn.execute("INSERT INTO round_failures VALUES (200, '0xc')")  # 0xc failed at 200

    df = pos.load_positions(conn, "BTC", 100, w)
    assert set(df["address"].to_list()) == {"0xa", "0xc"}  # vault excluded
    change = pos.net_change(conn, "BTC", 100, 200, w)
    assert change == (2.0, 1)  # only 0xa: in both rounds, polled OK, not excluded
    w_all = WalletsConfig(exclude_vaults=False)
    assert pos.net_change(conn, "BTC", 100, 200, w_all) == (-98.0, 2)


def test_active_at_follows_events(conn: sqlite3.Connection) -> None:
    with store.transaction(conn):
        ws.set_wallet(conn, "0xa", "active", "t", 10)
        ws.set_wallet(conn, "0xa", "dormant", "t", 20)
        ws.set_wallet(conn, "0xa", "active", "t", 30)
    events = ws.load_events(conn)
    assert ws.active_at(events, 15) == {"0xa"}
    assert ws.active_at(events, 25) == set()
    assert ws.active_at(events, 35) == {"0xa"}
    assert ws.active_at(events, 5) == set()
