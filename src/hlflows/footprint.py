"""Footprint: taker buy and sell volume per price level within each bar.

The recorder stores it per 15 minutes at a fixed price step per coin (footprint.tick)
whenever it writes an hour of raw trades to Parquet, so the dashboard never re-reads raw
trades. The dashboard adds the trades still staged, groups the 15m rows into its bars and
merges levels into a coarser step, chosen so a typical bar spans about footprint.levels
rows.
"""

from __future__ import annotations

import math
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl

from hlflows import store
from hlflows.config import FootprintConfig
from hlflows.timeutil import HOUR_MS, MINUTE_MS, to_ms

FP_MS = 15 * MINUTE_MS
_EPS = 1e-9  # px / tick can land a hair below an exact level in floating point
_TRADE_COLUMNS = ["ts", "side", "px", "sz"]


def tick_for(cfg: FootprintConfig, market: str) -> float | None:
    """Price step for a market ('BTC' or 'BTC/spot': both use the coin's tick)."""
    return cfg.tick.get(market.split("/")[0])


def levels(trades: pl.DataFrame, tick: float) -> pl.DataFrame:
    """(ts: 15m start, lvl, buy_sz, sell_sz) from raw trades (ts, side, px, sz).
    Level `lvl` covers prices [lvl * tick, (lvl + 1) * tick)."""
    return (
        trades.lazy()
        .group_by(
            (pl.col("ts") // FP_MS * FP_MS).alias("ts"),
            (pl.col("px") / tick + _EPS).floor().cast(pl.Int64).alias("lvl"),
        )
        .agg(
            pl.col("sz").filter(pl.col("side") == "B").sum().alias("buy_sz"),
            pl.col("sz").filter(pl.col("side") == "A").sum().alias("sell_sz"),
        )
        .sort("ts", "lvl")
        .collect()
    )


def replace_hour(
    conn: sqlite3.Connection, market: str, hour: int, tick: float, trades: pl.DataFrame
) -> int:
    """Store the footprint of one hour of a market's trades, replacing what was there (an
    hour is written again when late trades are merged into its Parquet file)."""
    rows = levels(trades, tick)
    conn.execute(
        "DELETE FROM footprint WHERE market = ? AND ts >= ? AND ts < ?",
        (market, hour, hour + HOUR_MS),
    )
    conn.executemany(
        "INSERT INTO footprint (market, ts, lvl, tick, buy_sz, sell_sz) VALUES (?, ?, ?, ?, ?, ?)",
        [(market, ts, lvl, tick, b, s) for ts, lvl, b, s in rows.iter_rows()],
    )
    return rows.height


def load(conn: sqlite3.Connection, market: str, start: int, end: int, tick: float) -> pl.DataFrame:
    """Footprint rows (ts, px: level floor price, buy_sz, sell_sz) in [start, end): the
    stored hours plus the trades still staged (the current hour, and any late trades not
    yet merged into their hour)."""
    schema = {"ts": pl.Int64, "px": pl.Float64, "buy_sz": pl.Float64, "sell_sz": pl.Float64}
    stored = pl.DataFrame(
        [tuple(r) for r in conn.execute(
            "SELECT ts, lvl * tick, buy_sz, sell_sz FROM footprint"
            " WHERE market = ? AND ts >= ? AND ts < ?",
            (market, start, end),
        )],
        schema=schema,
        orient="row",
    )  # fmt: skip
    staged = pl.DataFrame(
        [tuple(r) for r in conn.execute(
            "SELECT ts, side, px, sz FROM trades_staging WHERE market = ? AND ts >= ? AND ts < ?",
            (market, start, end),
        )],
        schema={"ts": pl.Int64, "side": pl.Utf8, "px": pl.Float64, "sz": pl.Float64},
        orient="row",
    )  # fmt: skip
    live = levels(staged, tick).select(
        "ts", (pl.col("lvl") * tick).alias("px"), "buy_sz", "sell_sz"
    )
    return pl.concat([stored, live])


def pick_step(tick: float, ranges: list[float], target_levels: int) -> float:
    """The smallest step in tick x (1, 2, 5, 10, 20, 50, ...) that splits the median bar
    range into at most `target_levels` levels. Whole multiples of the tick only, so every
    stored level falls in exactly one displayed level."""
    ranges = sorted(r for r in ranges if r > 0)
    if not ranges:
        return tick
    want = ranges[len(ranges) // 2] / target_levels
    scale = 1
    while True:
        for m in (1, 2, 5):
            if tick * m * scale >= want - _EPS * tick:
                return tick * m * scale
        scale *= 10


def _sig(x: float, digits: int = 4) -> float:
    return float(f"{x:.{digits}g}")


def series(
    rows: pl.DataFrame, bar_ts: list[int], bar_ms: int, ranges: list[float], tick: float,
    target_levels: int,
) -> dict[str, Any] | None:  # fmt: skip
    """Per bar (aligned with `bar_ts`): the lowest level index `lo` (level k covers
    [(lo + k) * step, (lo + k + 1) * step)), taker buys and sells per level from there up
    (coin units), and the point of control (middle of the busiest level). None for bars
    without trades."""
    if rows.is_empty() or not bar_ts:
        return None
    step = pick_step(tick, ranges, target_levels)
    agg = (
        rows.lazy()
        .filter(pl.col("ts") >= bar_ts[0])
        .group_by(
            (pl.col("ts") - (pl.col("ts") - bar_ts[0]) % bar_ms).alias("bar"),
            (pl.col("px") / step + _EPS).floor().cast(pl.Int64).alias("idx"),
        )
        .agg(pl.col("buy_sz").sum(), pl.col("sell_sz").sum())
        .sort("bar", "idx")
        .collect()
    )
    per_bar: dict[int, list[tuple[int, float, float]]] = {}
    for bar, idx, b, s in agg.iter_rows():
        per_bar.setdefault(bar, []).append((idx, b, s))
    lo: list[int | None] = []
    buy: list[list[float] | None] = []
    sell: list[list[float] | None] = []
    poc: list[float | None] = []
    for ts in bar_ts:
        cells = per_bar.get(ts)
        if not cells:
            lo.append(None)
            buy.append(None)
            sell.append(None)
            poc.append(None)
            continue
        first = cells[0][0]
        bs, ss = [0.0] * (cells[-1][0] - first + 1), [0.0] * (cells[-1][0] - first + 1)
        for idx, b, s in cells:
            bs[idx - first], ss[idx - first] = _sig(b), _sig(s)
        busiest = max(cells, key=lambda c: c[1] + c[2])[0]
        lo.append(first)
        buy.append(bs)
        sell.append(ss)
        poc.append(_sig((busiest + 0.5) * step, 8))
    return {"step": step, "lo": lo, "buy": buy, "sell": sell, "poc": poc}


def _hour_of(path: Path) -> int | None:
    """Start of the hour a raw-trade file holds (.../date=YYYY-MM-DD/hour=HH.parquet)."""
    try:
        day = datetime.strptime(path.parent.name.removeprefix("date="), "%Y-%m-%d")
        return to_ms(day.replace(tzinfo=UTC)) + int(path.stem.removeprefix("hour=")) * HOUR_MS
    except ValueError:
        return None


def rebuild(conn: sqlite3.Connection, trades_dir: Path, cfg: FootprintConfig, since: int) -> int:
    """Rebuild the stored footprint from the raw-trade Parquet files of hours starting at
    or after `since` (e.g. once after upgrading, or after changing footprint.tick). One
    short transaction per file, so it can run while the recorder writes. Returns the
    number of hours rebuilt."""
    done = 0
    for market_path in sorted(trades_dir.glob("market=*")):
        market = market_path.name.removeprefix("market=").replace("_", "/")
        tick = tick_for(cfg, market)
        if tick is None:
            continue
        for path in sorted(market_path.glob("date=*/hour=*.parquet")):
            hour = _hour_of(path)
            if hour is None or hour < since:
                continue
            trades = pl.read_parquet(path, columns=_TRADE_COLUMNS)
            with store.transaction(conn):
                replace_hour(conn, market, hour, tick, trades)
            done += 1
    return done


def prune(conn: sqlite3.Connection, before: int) -> int:
    """Delete stored footprint older than `before`."""
    with store.transaction(conn):
        return conn.execute("DELETE FROM footprint WHERE ts < ?", (before,)).rowcount


def bar_ranges(highs: list[float | None], lows: list[float | None]) -> list[float]:
    return [h - lo for h, lo in zip(highs, lows, strict=True)
            if h is not None and lo is not None and math.isfinite(h - lo)]  # fmt: skip
