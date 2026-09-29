from __future__ import annotations

import sqlite3

from hlflows import db, store
from hlflows.api import types as t
from tests.conftest import fixture_bytes


def _bar(source: str, c: float, buy: float | None = None) -> store.Bar:
    return store.Bar("BTC", "1m", 60_000, 1, 2, 0.5, c, 10, 3, buy, buy, None, None, source, None)


def _rows(conn: sqlite3.Connection, table: str) -> list[tuple[object, ...]]:
    return [tuple(r) for r in conn.execute(f"SELECT * FROM {table} ORDER BY 1, 2, 3")]


def test_migrations_are_idempotent(conn: sqlite3.Connection) -> None:
    version = conn.execute("PRAGMA user_version").fetchone()[0]
    assert version == len(db.MIGRATIONS)
    db.migrate(conn)
    assert conn.execute("PRAGMA user_version").fetchone()[0] == version


def test_bar_upsert_is_idempotent(conn: sqlite3.Connection) -> None:
    candles = t.decode(fixture_bytes("candleSnapshot_ETH_1h.json"), list[t.Candle])
    bars = store.bars_from_candles("ETH", "1h", candles, None)
    store.upsert_bars(conn, bars)
    first = _rows(conn, "bars")
    store.upsert_bars(conn, bars)
    assert _rows(conn, "bars") == first
    assert len(first) == len(candles)


def test_candle_never_overwrites_recorded_bar(conn: sqlite3.Connection) -> None:
    store.upsert_bars(conn, [_bar("ws", 1.5, buy=4.0)])
    store.upsert_bars(conn, [_bar("candle", 1.7)])
    row = conn.execute("SELECT c, buy_sz, source FROM bars").fetchone()
    assert tuple(row) == (1.5, 4.0, "ws")
    store.upsert_bars(conn, [_bar("candle", 1.7)])
    store.upsert_bars(conn, [_bar("ws", 1.6, buy=5.0)])  # a newer recorded bar wins
    assert tuple(conn.execute("SELECT c, buy_sz FROM bars").fetchone()) == (1.6, 5.0)


def test_funding_upsert_floors_hour_and_is_idempotent(conn: sqlite3.Connection) -> None:
    records = t.decode(fixture_bytes("fundingHistory_HYPE.json"), list[t.FundingRecord])
    store.upsert_funding(conn, records)
    store.upsert_funding(conn, records)
    rows = conn.execute("SELECT ts, raw_time FROM funding").fetchall()
    assert len(rows) == len(records)
    assert all(r["ts"] % 3_600_000 == 0 and 0 <= r["raw_time"] - r["ts"] < 3_600_000 for r in rows)


def test_predicted_fundings_skip_unlisted_venues(conn: sqlite3.Connection) -> None:
    data = t.decode(fixture_bytes("predictedFundings.json"), t.PredictedFundings)
    n = store.insert_predicted_fundings(conn, 1000, data, {"BTC", "HYPE"})
    n2 = store.insert_predicted_fundings(conn, 1000, data, {"BTC", "HYPE"})
    assert n == n2 > 0
    rows = conn.execute("SELECT DISTINCT coin FROM predicted_funding").fetchall()
    assert {r[0] for r in rows} == {"BTC", "HYPE"}
    assert conn.execute("SELECT COUNT(*) FROM predicted_funding").fetchone()[0] == n
