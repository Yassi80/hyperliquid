from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from pathlib import Path

from hlflows import db, store
from hlflows.config import WalletsConfig
from hlflows.maintenance import archive_positions, backup, positions_day_path, prune_local
from hlflows.timeutil import DAY_MS, HOUR_MS, to_ms
from hlflows.wallets import positioning as pos
from hlflows.wallets import store as ws

D0 = to_ms(datetime(2026, 9, 1, tzinfo=UTC))


def add_round(conn: sqlite3.Connection, round_ts: int, szi: float) -> None:
    ws.insert_positions(conn, [ws.PositionRow(round_ts, "0xa", "BTC", szi, 1, abs(szi) * 100,
                                              "cross", 10, 5, 90, 0, 1, 1e4)])  # fmt: skip
    conn.execute("INSERT INTO snapshot_rounds VALUES (?, 1, 0, 1)", (round_ts,))


def test_archive_moves_whole_days_and_queries_still_work(
    conn: sqlite3.Connection, tmp_path: Path
) -> None:
    with store.transaction(conn):
        ws.set_wallet(conn, "0xa", "active", "t", 0)
        add_round(conn, D0 + HOUR_MS, 1)
        add_round(conn, D0 + 2 * HOUR_MS, 3)
        add_round(conn, D0 + DAY_MS + HOUR_MS, 5)  # the next day stays hot
    done = archive_positions(conn, tmp_path, D0 + DAY_MS + 12 * HOUR_MS)
    assert done == [(D0, 2)]
    assert conn.execute("SELECT COUNT(*) FROM positions").fetchone()[0] == 1
    assert positions_day_path(tmp_path, D0).exists()
    assert archive_positions(conn, tmp_path, D0 + DAY_MS) == []  # idempotent

    w = WalletsConfig()
    df = pos.load_positions(conn, "BTC", D0 + 2 * HOUR_MS, w, str(tmp_path))
    assert df["szi"].to_list() == [3.0]
    assert pos.load_positions(conn, "BTC", D0 + 2 * HOUR_MS, w).is_empty()  # no archive dir
    change = pos.net_change(conn, "BTC", D0 + HOUR_MS, D0 + DAY_MS + HOUR_MS, w, str(tmp_path))
    assert change == (4.0, 1)


def test_prune_local_only_old_partitions(tmp_path: Path) -> None:
    old = tmp_path / "trades" / "market=BTC" / "date=2026-08-01" / "hour=10.parquet"
    new = tmp_path / "trades" / "market=BTC" / "date=2026-09-28" / "hour=10.parquet"
    old_pos = tmp_path / "positions" / "date=2026-08-01.parquet"
    other = tmp_path / "exports" / "keep.parquet"
    for p in (old, new, old_pos, other):
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(b"x")
    now = datetime(2026, 9, 29, tzinfo=UTC)
    removed = prune_local(tmp_path, 30, now)
    assert set(removed) == {old, old_pos}
    assert new.exists() and other.exists()
    assert not old.parent.exists()  # empty partition directory removed
    assert prune_local(tmp_path, 0, now) == []


def test_backup_is_a_usable_copy(conn: sqlite3.Connection, tmp_path: Path) -> None:
    with store.transaction(conn):
        ws.set_wallet(conn, "0xa", "active", "t", 0)
    path = backup(conn, tmp_path / "b" / "copy.sqlite")
    copy = db.connect(path)
    assert copy.execute("SELECT address FROM wallets").fetchone()[0] == "0xa"
    assert copy.execute("PRAGMA user_version").fetchone()[0] == len(db.MIGRATIONS)
