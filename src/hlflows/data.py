"""Query layer: stored data resampled to any timeframe, as polars DataFrames.

The CLI (`show`, `export`) and future research code both read through these functions,
so bar alignment, coverage rules and funding normalization live in one place.

Bars are stored at 1m (recorded or candle), 1h and 1d (candle backfill). For each
output bar, the finest stored resolution whose coverage spans the whole bar is used.
If none does, the best-covered resolution is used and the bar is marked incomplete.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

import polars as pl

from hlflows import coverage as cov
from hlflows.funding import FundingStats, funding_stats, venue_rate_to_8h
from hlflows.timeutil import (
    DAY_MS,
    HOUR_MS,
    MINUTE_MS,
    WEEK_MS,
    WEEKDAYS,
    Timeframe,
    bucket_start,
    next_bucket_start,
    now_ms,
)

STORED_RES = {"1m": MINUTE_MS, "1h": HOUR_MS, "1d": DAY_MS}
TAKER_STREAM = "taker_1m"


def bars_stream(res: str) -> str:
    return f"bars_{res}"


def _usable_res(tf: Timeframe) -> list[str]:
    """Stored resolutions that tile `tf` exactly, finest first."""
    if tf.fixed_ms is None:  # months are whole days
        return list(STORED_RES)
    return [r for r, ms in STORED_RES.items() if tf.fixed_ms % ms == 0 and tf.fixed_ms >= ms]


def bucket_expr(tf: Timeframe, week_start: str) -> pl.Expr:
    ts = pl.col("ts")
    if tf.name == "1M":
        return pl.from_epoch(ts, time_unit="ms").dt.truncate("1mo").dt.epoch("ms")
    assert tf.fixed_ms is not None
    if tf.name == "1w":
        offset = ((WEEKDAYS.index(week_start) - 3) % 7) * DAY_MS
        return ((ts - offset) // WEEK_MS) * WEEK_MS + offset
    return (ts // tf.fixed_ms) * tf.fixed_ms


def bucket_starts(tf: Timeframe, start: int, end: int, week_start: str) -> list[int]:
    out = []
    b = bucket_start(start, tf, week_start)
    while b < end:
        out.append(b)
        b = next_bucket_start(b, tf)
    return out


BAR_SCHEMA = {
    "ts": pl.Int64,
    "o": pl.Float64,
    "h": pl.Float64,
    "l": pl.Float64,
    "c": pl.Float64,
    "volume": pl.Float64,
    "n": pl.Int64,
    "buy_sz": pl.Float64,
    "sell_sz": pl.Float64,
    "buy_ntl": pl.Float64,
    "sell_ntl": pl.Float64,
}


def _read_bars(
    conn: sqlite3.Connection, market: str, res: str, start: int, end: int
) -> pl.DataFrame:
    rows = conn.execute(
        """SELECT ts, o, h, l, c, volume, n, buy_sz, sell_sz, buy_ntl, sell_ntl FROM bars
           WHERE market = ? AND res = ? AND ts >= ? AND ts < ? ORDER BY ts""",
        (market, res, start, end),
    ).fetchall()
    return pl.DataFrame([tuple(r) for r in rows], schema=BAR_SCHEMA, orient="row")


def load_bars(
    conn: sqlite3.Connection,
    market: str,
    tf: Timeframe,
    start: int,
    end: int | None = None,
    week_start: str = "monday",
) -> pl.DataFrame:
    """OHLCV + taker split per `tf` bar for bars opening in [start, end).

    Columns: ts, o, h, l, c, volume, n, buy_sz, sell_sz, delta, buy_ntl, sell_ntl,
    res (stored resolution used for OHLCV), complete (coverage spans the bar),
    taker_cov (share of the bar with a confirmed taker split, 0-1), taker (taker_cov == 1),
    closed (bar has ended), quality.

    The taker split always comes from recorded 1m bars, whichever resolution supplies
    OHLCV. It sums every recorded minute, so a bar missing a minute or two (a reconnect)
    keeps its delta with `taker_cov` < 1 and quality `taker_partial`; it is None only when
    no minute in the bar has a taker split. Bars with no data at any resolution are omitted.
    """
    end = now_ms() if end is None else end
    starts = bucket_starts(tf, start, end, week_start)
    if not starts:
        return _empty_bars()
    span_start, span_end = starts[0], next_bucket_start(starts[-1], tf)
    now = now_ms()

    significance = conn.execute(
        "SELECT significance FROM markets WHERE market = ?", (market,)
    ).fetchone()
    low_sig = significance is not None and significance[0] == "low"

    per_res: dict[str, dict[int, dict[str, object]]] = {}
    coverage: dict[str, list[cov.Interval]] = {}
    for res in _usable_res(tf):
        coverage[res] = cov.get_coverage(conn, bars_stream(res), market, span_start, span_end)
        df = _read_bars(conn, market, res, span_start, span_end)
        if df.is_empty():
            per_res[res] = {}
            continue
        agg = (
            df.sort("ts")
            .with_columns(bucket_expr(tf, week_start).alias("bucket"))
            .group_by("bucket")
            .agg(
                pl.col("o").first(),
                pl.col("h").max(),
                pl.col("l").min(),
                pl.col("c").last(),
                pl.col("volume").sum(),
                pl.col("n").sum(),
                pl.col("buy_sz").sum(),
                pl.col("sell_sz").sum(),
                pl.col("buy_ntl").sum(),
                pl.col("sell_ntl").sum(),
                pl.col("buy_sz").is_not_null().sum().alias("taker_rows"),
            )
        )
        per_res[res] = {row["bucket"]: row for row in agg.iter_rows(named=True)}
    taker_cov = cov.get_coverage(conn, TAKER_STREAM, market, span_start, span_end)

    out: list[dict[str, object]] = []
    for b in starts:
        b_end = next_bucket_start(b, tf)
        closed = b_end <= now
        span = (b, min(b_end, now))
        chosen: str | None = None
        complete = False
        for res, rows in per_res.items():
            if b in rows and cov.contains(coverage[res], span):
                chosen, complete = res, True
                break
        if chosen is None:
            # No resolution covers the whole bar: take the one covering the most of it.
            best = -1
            for res, rows in per_res.items():
                if b not in rows:
                    continue
                covered = (span[1] - span[0]) - sum(
                    e - s for s, e in cov.subtract(span, coverage[res])
                )
                if covered > best:
                    chosen, best = res, covered
        if chosen is None:
            continue
        row = per_res[chosen][b]
        minute = per_res.get("1m", {}).get(b)
        has_taker = minute is not None and int(minute["taker_rows"]) > 0  # type: ignore[call-overload]
        span_ms = span[1] - span[0]
        uncovered = sum(e - s for s, e in cov.subtract(span, taker_cov))
        taker_frac = (span_ms - uncovered) / span_ms if has_taker and span_ms > 0 else 0.0
        quality = []
        if not complete:
            quality.append("partial")
        if has_taker and taker_frac < 1:
            quality.append("taker_partial")
        if low_sig:
            quality.append("low_significance")
        split = minute if has_taker else None
        out.append(
            {
                "ts": b,
                "o": row["o"],
                "h": row["h"],
                "l": row["l"],
                "c": row["c"],
                "volume": row["volume"],
                "n": row["n"],
                "buy_sz": split["buy_sz"] if split else None,
                "sell_sz": split["sell_sz"] if split else None,
                "buy_ntl": split["buy_ntl"] if split else None,
                "sell_ntl": split["sell_ntl"] if split else None,
                "res": chosen,
                "complete": complete,
                "taker_cov": taker_frac if has_taker else None,
                "taker": has_taker and taker_frac >= 1,
                "closed": closed,
                "quality": ",".join(quality) or None,
            }
        )
    if not out:
        return _empty_bars()
    return (
        pl.DataFrame(out, schema=_OUT_SCHEMA)
        .with_columns((pl.col("buy_sz") - pl.col("sell_sz")).alias("delta"))
        .select(_OUT_COLUMNS)
    )


_OUT_SCHEMA = {
    **BAR_SCHEMA,
    "res": pl.Utf8,
    "complete": pl.Boolean,
    "taker_cov": pl.Float64,
    "taker": pl.Boolean,
    "closed": pl.Boolean,
    "quality": pl.Utf8,
}
_OUT_COLUMNS = [
    "ts",
    "o",
    "h",
    "l",
    "c",
    "volume",
    "n",
    "buy_sz",
    "sell_sz",
    "delta",
    "buy_ntl",
    "sell_ntl",
    "res",
    "complete",
    "taker_cov",
    "taker",
    "closed",
    "quality",
]


def _empty_bars() -> pl.DataFrame:
    return (
        pl.DataFrame(schema=_OUT_SCHEMA)
        .with_columns(pl.lit(None, dtype=pl.Float64).alias("delta"))
        .select(_OUT_COLUMNS)
    )


def load_ctx(
    conn: sqlite3.Connection,
    coin: str,
    tf: Timeframe,
    start: int,
    end: int | None = None,
    week_start: str = "monday",
) -> pl.DataFrame:
    """Asset context per bar: OI (coin units) at first/last sample, last mark, mean premium.

    Columns: ts, oi_open, oi_close, mark_close, premium_mean, samples.
    """
    end = now_ms() if end is None else end
    rows = conn.execute(
        """SELECT ts, oi, mark_px, premium FROM asset_ctx
           WHERE coin = ? AND ts >= ? AND ts < ? ORDER BY ts""",
        (coin, bucket_start(start, tf, week_start), end),
    ).fetchall()
    schema = {"ts": pl.Int64, "oi": pl.Float64, "mark_px": pl.Float64, "premium": pl.Float64}
    df = pl.DataFrame([tuple(r) for r in rows], schema=schema, orient="row")
    return (
        df.with_columns(bucket_expr(tf, week_start).alias("bucket"))
        .group_by("bucket")
        .agg(
            pl.col("oi").first().alias("oi_open"),
            pl.col("oi").last().alias("oi_close"),
            pl.col("mark_px").last().alias("mark_close"),
            pl.col("premium").mean().alias("premium_mean"),
            pl.len().alias("samples"),
        )
        .rename({"bucket": "ts"})
        .sort("ts")
    )


def load_funding(
    conn: sqlite3.Connection, coin: str, start: int, end: int | None = None
) -> pl.DataFrame:
    """Hourly realized funding. Columns: ts, rate_1h, rate_8h, premium."""
    end = now_ms() if end is None else end
    rows = conn.execute(
        "SELECT ts, rate_1h, premium FROM funding"
        " WHERE coin = ? AND ts >= ? AND ts < ? ORDER BY ts",
        (coin, start, end),
    ).fetchall()
    schema = {"ts": pl.Int64, "rate_1h": pl.Float64, "premium": pl.Float64}
    df = pl.DataFrame([tuple(r) for r in rows], schema=schema, orient="row")
    return df.with_columns((pl.col("rate_1h") * 8).alias("rate_8h")).select(
        "ts", "rate_1h", "rate_8h", "premium"
    )


def load_funding_bars(
    conn: sqlite3.Connection,
    coin: str,
    tf: Timeframe,
    start: int,
    end: int | None = None,
    week_start: str = "monday",
) -> pl.DataFrame:
    """Funding per bar: mean hourly rate, its 8h equivalent, mean premium, hours counted.

    For bars shorter than an hour, each bar gets the rate of the hour it falls in.
    """
    end = now_ms() if end is None else end
    hourly = load_funding(conn, coin, bucket_start(start, tf, week_start) - HOUR_MS, end)
    if tf.fixed_ms is not None and tf.fixed_ms < HOUR_MS:
        starts = bucket_starts(tf, start, end, week_start)
        grid = pl.DataFrame({"ts": starts}, schema={"ts": pl.Int64}).with_columns(
            ((pl.col("ts") // HOUR_MS) * HOUR_MS).alias("hour")
        )
        return (
            grid.join(hourly.rename({"ts": "hour"}), on="hour", how="inner")
            .with_columns(pl.lit(1, dtype=pl.UInt32).alias("hours"))
            .select("ts", "rate_1h", "rate_8h", "premium", "hours")
        )
    return (
        hourly.filter(pl.col("ts") >= bucket_start(start, tf, week_start))
        .with_columns(bucket_expr(tf, week_start).alias("bucket"))
        .group_by("bucket")
        .agg(
            pl.col("rate_1h").mean(),
            pl.col("rate_8h").mean(),
            pl.col("premium").mean(),
            pl.len().alias("hours"),
        )
        .rename({"bucket": "ts"})
        .sort("ts")
    )


def current_funding_stats(
    conn: sqlite3.Connection, coin: str, window_days: int, at: int | None = None
) -> FundingStats | None:
    """Latest realized funding with percentiles against the trailing window."""
    at = now_ms() if at is None else at
    df = load_funding(conn, coin, at - window_days * DAY_MS, at + 1)
    if df.is_empty():
        return None
    last = df.row(-1, named=True)
    return funding_stats(
        last["ts"],
        last["rate_1h"],
        last["premium"],
        df["rate_1h"].to_list(),
        df["premium"].to_list(),
    )


@dataclass(frozen=True)
class VenueFundingRow:
    venue: str
    rate: float
    interval_h: float
    rate_8h: float
    next_ts: int
    observed_ts: int


def latest_predicted_funding(conn: sqlite3.Connection, coin: str) -> list[VenueFundingRow]:
    rows = conn.execute(
        """SELECT venue, rate, interval_h, next_ts, observed_ts FROM predicted_funding
           WHERE coin = ? AND observed_ts = (
             SELECT MAX(observed_ts) FROM predicted_funding WHERE coin = ?)
           ORDER BY venue""",
        (coin, coin),
    ).fetchall()
    return [
        VenueFundingRow(
            r["venue"],
            r["rate"],
            r["interval_h"],
            venue_rate_to_8h(r["rate"], r["interval_h"]),
            r["next_ts"],
            r["observed_ts"],
        )
        for r in rows
    ]
