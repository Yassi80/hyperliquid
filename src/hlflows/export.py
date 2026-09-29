"""`export`: write query-layer DataFrames to CSV or Parquet."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import polars as pl

from hlflows import data
from hlflows.config import Config
from hlflows.markets import spot_market_id
from hlflows.timeutil import Timeframe
from hlflows.wallets import positioning as pos

TABLES = ("bars", "spot", "ctx", "funding", "positioning", "liquidations", "liqmap")


def build(
    conn: sqlite3.Connection,
    cfg: Config,
    table: str,
    coin: str,
    tf: Timeframe,
    start: int,
    end: int,
) -> pl.DataFrame:
    """Tables resampled to `tf`: bars, spot, ctx, funding. Per polling round (tf ignored):
    positioning. Per event: liquidations. At the last round before `end`: liqmap."""
    week_start = cfg.time.week_start
    if table == "positioning":
        df = pos.positioning_series(conn, coin, start, end, cfg.wallets, cfg.storage.data_dir)
    elif table == "liquidations":
        df = pos.load_liquidations(conn, coin, start, end)
    elif table == "liqmap":
        from hlflows.liqmap import liqmap_frame

        round_ts = pos.latest_round(conn, end)
        df = liqmap_frame(cfg, conn, coin, end)
        if df.is_empty() or round_ts is None:
            raise ValueError("no positioning snapshot before --to")
        df = df.with_columns(pl.lit(round_ts, dtype=pl.Int64).alias("ts"))
    elif table == "bars":
        df = data.load_bars(conn, coin, tf, start, end, week_start)
    elif table == "spot":
        df = data.load_bars(conn, spot_market_id(coin), tf, start, end, week_start)
    elif table == "ctx":
        df = data.load_ctx(conn, coin, tf, start, end, week_start)
    elif table == "funding":
        df = data.load_funding_bars(conn, coin, tf, start, end, week_start)
    else:
        raise ValueError(f"unknown table {table!r}; choose from {', '.join(TABLES)}")
    return df.with_columns(
        pl.from_epoch("ts", time_unit="ms").dt.replace_time_zone("UTC").alias("time_utc")
    ).select("time_utc", pl.exclude("time_utc"))


def write(df: pl.DataFrame, path: Path, fmt: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    if fmt == "csv":
        df.write_csv(tmp)
    elif fmt == "parquet":
        df.write_parquet(tmp, compression="zstd")
    else:
        raise ValueError(f"unknown format {fmt!r}; choose csv or parquet")
    tmp.replace(path)
