"""Raw trades: staging in SQLite, 1m bars with the taker split, hourly Parquet files.

Trades are deduplicated by (market, tid) on insert, so the batch the trades channel
replays on every (re)subscribe never double counts. Bars are always built from the
staging table, never from in-memory state, so a restart or a late trade just means
rebuilding the affected minute.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable, Mapping
from pathlib import Path

import polars as pl

from hlflows.api import types as t
from hlflows.store import Bar
from hlflows.timeutil import HOUR_MS, MINUTE_MS, floor_ms, from_ms

MinuteKey = tuple[str, int]  # (market, minute start)

TRADE_COLUMNS = ("tid", "ts", "side", "px", "sz", "buyer", "seller", "hash")
TRADE_SCHEMA = {
    "tid": pl.Int64,
    "ts": pl.Int64,
    "side": pl.Utf8,
    "px": pl.Float64,
    "sz": pl.Float64,
    "buyer": pl.Utf8,
    "seller": pl.Utf8,
    "hash": pl.Utf8,
}


def insert_trades(
    conn: sqlite3.Connection, market_by_hl: Mapping[str, str], trades: Iterable[t.WsTrade]
) -> tuple[list[tuple[str, t.WsTrade]], set[MinuteKey]]:
    """Stage trades. Returns ((market, trade) actually new, minutes that gained trades).

    Replayed trades already staged are skipped, so callers can count the new ones.
    Trades already moved to Parquet can't be detected here and come back as new."""
    rows = []
    kept: list[t.WsTrade] = []
    for tr in trades:
        market = market_by_hl.get(tr.coin)
        if market is None or tr.side not in ("A", "B"):
            continue
        rows.append(
            (market, tr.tid, tr.time, tr.side, tr.px, tr.sz, tr.users[0], tr.users[1], tr.hash)
        )
        kept.append(tr)
    touched: set[MinuteKey] = set()
    inserted: list[tuple[str, t.WsTrade]] = []
    for row, tr in zip(rows, kept, strict=True):
        cur = conn.execute(
            """INSERT OR IGNORE INTO trades_staging
               (market, tid, ts, side, px, sz, buyer, seller, hash)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            row,
        )
        if cur.rowcount:
            inserted.append((row[0], tr))
            touched.add((row[0], floor_ms(row[2], MINUTE_MS)))
    return inserted, touched


def aggregate_minute(
    market: str,
    minute: int,
    trades: Iterable[tuple[float, float, str]],
    partial: bool,
    low_significance: bool = False,
) -> Bar | None:
    """Build one 1m bar from (px, sz, side) tuples in trade order."""
    o = h = l = c = 0.0  # noqa: E741
    volume = buy_sz = sell_sz = buy_ntl = sell_ntl = 0.0
    n = 0
    for px, sz, side in trades:
        if n == 0:
            o = h = l = px  # noqa: E741
        h, l, c = max(h, px), min(l, px), px  # noqa: E741
        volume += sz
        if side == "B":
            buy_sz += sz
            buy_ntl += px * sz
        else:
            sell_sz += sz
            sell_ntl += px * sz
        n += 1
    if n == 0:
        return None
    quality = [q for q, on in (("partial", partial), ("low_significance", low_significance)) if on]
    return Bar(
        market, "1m", minute, o, h, l, c, volume, n,
        buy_sz, sell_sz, buy_ntl, sell_ntl, "ws", ",".join(quality) or None,
    )  # fmt: skip


def build_bar(
    conn: sqlite3.Connection, market: str, minute: int, partial: bool, low_significance: bool
) -> Bar | None:
    rows = conn.execute(
        """SELECT px, sz, side FROM trades_staging
           WHERE market = ? AND ts >= ? AND ts < ? ORDER BY ts, tid""",
        (market, minute, minute + MINUTE_MS),
    ).fetchall()
    return aggregate_minute(
        market, minute, ((r[0], r[1], r[2]) for r in rows), partial, low_significance
    )


def market_dir(market: str) -> str:
    return "market=" + market.replace("/", "_")


def hour_path(trades_dir: Path, market: str, hour: int) -> Path:
    dt = from_ms(hour)
    return trades_dir / market_dir(market) / f"date={dt:%Y-%m-%d}" / f"hour={dt:%H}.parquet"


def flush_raw_trades(
    conn: sqlite3.Connection, trades_dir: Path, before: int
) -> list[tuple[str, int, int]]:
    """Write every staged hour that ended before `before` to Parquet, then unstage it.

    Idempotent: if a file for the hour already exists (e.g. a crash between writing it
    and deleting the staged rows), rows are merged and deduplicated by tid.
    Returns (market, hour, rows in file) per hour written.
    """
    hours = conn.execute(
        """SELECT DISTINCT market, (ts / ?) * ? AS hour FROM trades_staging
           WHERE ts < ? ORDER BY market, hour""",
        (HOUR_MS, HOUR_MS, floor_ms(before, HOUR_MS)),
    ).fetchall()
    written = []
    for market, hour in hours:
        rows = conn.execute(
            f"""SELECT {", ".join(TRADE_COLUMNS)} FROM trades_staging
                WHERE market = ? AND ts >= ? AND ts < ?""",
            (market, hour, hour + HOUR_MS),
        ).fetchall()
        df = pl.DataFrame([tuple(r) for r in rows], schema=TRADE_SCHEMA, orient="row")
        path = hour_path(trades_dir, market, hour)
        if path.exists():
            df = pl.concat([pl.read_parquet(path), df])
        df = df.unique(subset="tid", keep="first").sort("ts", "tid")
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".parquet.tmp")
        df.write_parquet(tmp, compression="zstd")
        tmp.replace(path)
        conn.execute(
            "DELETE FROM trades_staging WHERE market = ? AND ts >= ? AND ts < ?",
            (market, hour, hour + HOUR_MS),
        )
        written.append((market, hour, df.height))
    return written


def read_raw_trades(trades_dir: Path, market: str, start: int, end: int) -> pl.DataFrame:
    """Raw trades for a market from the hourly Parquet files (for research / rebuilds)."""
    root = trades_dir / market_dir(market)
    files = sorted(root.glob("date=*/hour=*.parquet"))
    if not files:
        return pl.DataFrame(schema=TRADE_SCHEMA)
    return (
        pl.scan_parquet(files, hive_partitioning=False)
        .filter((pl.col("ts") >= start) & (pl.col("ts") < end))
        .sort("ts", "tid")
        .collect()
    )
