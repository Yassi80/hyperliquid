from __future__ import annotations

import json
import re
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

from hlflows import coverage as cov
from hlflows import dashboard as dash
from hlflows import data, store
from hlflows.config import Config, StorageConfig
from hlflows.timeutil import HOUR_MS, MINUTE_MS, TIMEFRAMES, now_ms, to_ms
from hlflows.wallets import store as ws


def test_cvd_breaks_on_missing_delta_and_resumes() -> None:
    assert dash._cvd([1.0, 2.0, None, -4.0, 1.0]) == [1.0, 3.0, None, -1.0, 0.0]


def test_round_at_close_within_slack() -> None:
    rounds = [100, 220, 340]
    assert dash._round_at_close(rounds, 350, 50) == 340
    assert dash._round_at_close(rounds, 340, 150) == 220  # a round at the close is excluded
    assert dash._round_at_close(rounds, 1000, 50) is None
    assert dash._round_at_close(rounds, 50, 50) is None


def _seed(conn: sqlite3.Connection) -> None:
    """Two hours of recorded 1m bars for BTC ending at the last full hour, and a
    positioning round just before each hour's close, so every panel has data."""
    end = (now_ms() // HOUR_MS) * HOUR_MS
    start = end - 2 * HOUR_MS
    bars = [
        store.Bar("BTC", "1m", start + i * MINUTE_MS, 100, 101, 99, 100.5, 2, 3, 1.2, 0.8,
                  120, 80, "ws", None)
        for i in range(120)
    ]  # fmt: skip
    store.upsert_bars(conn, bars)
    for stream in (data.bars_stream("1m"), data.TAKER_STREAM):
        cov.add_coverage(conn, stream, "BTC", start, end)
    ws.set_wallet(conn, "0xa", "active", "t", start - 10)
    for r, szi in ((end - HOUR_MS - 60_000, 1.0), (end - 60_000, 3.0)):
        ws.insert_positions(conn, [ws.PositionRow(r, "0xa", "BTC", szi, 100, abs(szi) * 100,
                                                  "cross", 5, 5, 90, 0, 1, 1e4)])  # fmt: skip
        conn.execute("INSERT INTO snapshot_rounds VALUES (?, 1, 0, 1)", (r,))


def test_build_and_render(conn: sqlite3.Connection, tmp_path: Path) -> None:
    with store.transaction(conn):
        _seed(conn)
    cfg = Config(storage=StorageConfig(db_path=":memory:", data_dir=str(tmp_path)))
    payload = dash.build(cfg, conn, ["BTC"], [TIMEFRAMES["15m"], TIMEFRAMES["1h"]], 8)
    t = payload["coins"]["BTC"]["tfs"]["1h"]
    assert t["closed"] == [True, True]  # two recorded hours, nothing in the current one
    assert t["delta"] == [24.0, 24.0]  # 60 x (1.2 - 0.8)
    assert t["cvd"] == [24.0, 48.0]
    assert t["watch_long"] == [100.0, 300.0]
    assert t["watch_flow"] == [0.0, 2.0 * 100.5]  # +2 BTC over the second hour, at its close

    html = dash.render_html(payload)
    blob = re.search(r"const DATA = (\{.*?\});\n", html, re.S)
    assert blob is not None
    decoded = json.loads(blob.group(1).replace("<\\/", "</"))
    assert decoded["tfs"] == ["15m", "1h"]
    assert "NaN" not in blob.group(1)


def test_clean_replaces_non_finite() -> None:
    assert dash._clean({"a": [1.0, float("nan"), float("inf")], "b": (2.0,)}) == {
        "a": [1.0, None, None],
        "b": [2.0],
    }
    assert datetime.now(UTC) and to_ms(datetime(2026, 1, 1, tzinfo=UTC)) > 0
