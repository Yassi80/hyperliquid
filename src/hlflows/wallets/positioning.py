"""Watchlist positioning: summaries, leverage distribution, liquidation map, coverage.

Coverage: open interest counts one side (total longs = total shorts), so long coverage
is watchlist long notional / OI notional, short coverage likewise, and combined coverage
is (long + short) / (2 x OI notional).
"""

from __future__ import annotations

import math
import sqlite3
from dataclasses import dataclass

import polars as pl

from hlflows.config import WalletsConfig
from hlflows.maintenance import read_archived_positions
from hlflows.timeutil import MINUTE_MS
from hlflows.wallets import store as ws

POSITION_SCHEMA = {
    "address": pl.Utf8,
    "szi": pl.Float64,
    "position_value": pl.Float64,
    "liq_px": pl.Float64,
    "lev": pl.Float64,
    "eff_lev": pl.Float64,
    "lev_type": pl.Utf8,
    "account_value": pl.Float64,
}
LEVERAGE_BUCKETS = [(0, 2), (2, 5), (5, 10), (10, 20), (20, math.inf)]


def excluded_sql(w: WalletsConfig) -> str:
    """SQL condition (on alias `w`) for wallets left out of aggregates."""
    parts = []
    if w.exclude_vaults:
        parts.append("(COALESCE(w.tags, '') LIKE '%vault%' OR COALESCE(w.role, '') = 'vault')")
    if w.exclude_mm:
        parts.append("COALESCE(w.tags, '') LIKE '%mm%'")
    return " OR ".join(parts) if parts else "0"


def latest_round(conn: sqlite3.Connection, at: int | None = None) -> int | None:
    sql, args = "SELECT MAX(round_ts) FROM snapshot_rounds", ()
    if at is not None:
        sql, args = sql + " WHERE round_ts <= ?", (at,)  # type: ignore[assignment]
    row = conn.execute(sql, args).fetchone()
    return None if row[0] is None else int(row[0])


def excluded_addresses(conn: sqlite3.Connection, w: WalletsConfig) -> set[str]:
    return {r[0] for r in conn.execute(f"SELECT w.address FROM wallets w WHERE {excluded_sql(w)}")}


def round_positions(
    conn: sqlite3.Connection, round_ts: int, coin: str, data_dir: str | None = None
) -> pl.DataFrame:
    """All stored positions for one round and coin: from SQLite, or from the Parquet
    archive (`hlflows maintain`) once the round has been moved there."""
    cols = list(POSITION_SCHEMA)
    rows = conn.execute(
        f"SELECT {', '.join(cols)} FROM positions WHERE round_ts = ? AND coin = ?",
        (round_ts, coin),
    ).fetchall()
    if rows or data_dir is None:
        return pl.DataFrame([tuple(r) for r in rows], schema=POSITION_SCHEMA, orient="row")
    return read_archived_positions(data_dir, round_ts, coin).select(cols)


def load_positions(
    conn: sqlite3.Connection,
    coin: str,
    round_ts: int,
    w: WalletsConfig,
    data_dir: str | None = None,
) -> pl.DataFrame:
    """One round's positions in `coin`, without excluded wallets (vaults, market makers)."""
    df = round_positions(conn, round_ts, coin, data_dir)
    excluded = excluded_addresses(conn, w)
    return df.filter(~pl.col("address").is_in(list(excluded))) if excluded else df


@dataclass(frozen=True)
class MarketRef:
    ts: int
    oi: float  # coin units
    mark: float

    @property
    def oi_ntl(self) -> float:
        return self.oi * self.mark


def market_ref(conn: sqlite3.Connection, coin: str, at: int) -> MarketRef | None:
    """Latest asset context at or before `at` (within 10 minutes)."""
    row = conn.execute(
        """SELECT ts, oi, mark_px FROM asset_ctx WHERE coin = ? AND ts <= ? AND ts > ?
           ORDER BY ts DESC LIMIT 1""",
        (coin, at, at - 10 * MINUTE_MS),
    ).fetchone()
    return None if row is None else MarketRef(row[0], row[1], row[2])


@dataclass(frozen=True)
class Summary:
    round_ts: int
    n_long: int
    n_short: int
    long_ntl: float
    short_ntl: float
    long_cov: float | None
    short_cov: float | None
    combined_cov: float | None
    lev_long: float | None  # notional-weighted effective leverage
    lev_short: float | None
    no_liq_ntl: float  # positions without a liquidation price

    @property
    def net_ntl(self) -> float:
        return self.long_ntl - self.short_ntl


def _weighted_lev(df: pl.DataFrame) -> float | None:
    d = df.filter(pl.col("eff_lev").is_not_null())
    total = float(d["position_value"].sum())
    weighted = float((d["eff_lev"] * d["position_value"]).sum())
    return weighted / total if total else None


def summarize(df: pl.DataFrame, round_ts: int, ref: MarketRef | None) -> Summary:
    longs = df.filter(pl.col("szi") > 0)
    shorts = df.filter(pl.col("szi") < 0)
    long_ntl = float(longs["position_value"].sum())
    short_ntl = float(shorts["position_value"].sum())
    oi_ntl = ref.oi_ntl if ref is not None and ref.oi > 0 else None
    return Summary(
        round_ts=round_ts,
        n_long=longs.height,
        n_short=shorts.height,
        long_ntl=long_ntl,
        short_ntl=short_ntl,
        long_cov=long_ntl / oi_ntl if oi_ntl else None,
        short_cov=short_ntl / oi_ntl if oi_ntl else None,
        combined_cov=(long_ntl + short_ntl) / (2 * oi_ntl) if oi_ntl else None,
        lev_long=_weighted_lev(longs),
        lev_short=_weighted_lev(shorts),
        no_liq_ntl=float(df.filter(pl.col("liq_px").is_null())["position_value"].sum()),
    )


def leverage_distribution(df: pl.DataFrame) -> pl.DataFrame:
    """Notional and count per effective-leverage bucket, long and short."""
    rows = []
    for lo, hi in LEVERAGE_BUCKETS:
        sel = df.filter(
            pl.col("eff_lev").is_not_null() & (pl.col("eff_lev") >= lo) & (pl.col("eff_lev") < hi)
        )
        longs, shorts = sel.filter(pl.col("szi") > 0), sel.filter(pl.col("szi") < 0)
        rows.append({
            "bucket": f"{lo:g}-{hi:g}x" if hi != math.inf else f"{lo:g}x+",
            "long_ntl": float(longs["position_value"].sum()), "n_long": longs.height,
            "short_ntl": float(shorts["position_value"].sum()), "n_short": shorts.height,
        })  # fmt: skip
    return pl.DataFrame(rows)


def liqmap(df: pl.DataFrame, mark: float, bucket_pct: float, range_pct: float) -> pl.DataFrame:
    """Notional by liquidation-price bucket around `mark`.

    Bucket k covers liquidation prices in [mark*(1 + k*b), mark*(1 + (k+1)*b)) for bucket
    width b = bucket_pct/100, within +-range_pct. Longs liquidate below the mark, shorts
    above. `cum_*` accumulate outward from the mark: the notional that liquidates if price
    reaches that bucket.
    """
    b = bucket_pct / 100
    n = math.ceil(range_pct / bucket_pct)
    d = df.filter(pl.col("liq_px").is_not_null() & (pl.col("liq_px") > 0)).with_columns(
        ((pl.col("liq_px") / mark - 1) / b).floor().cast(pl.Int64).alias("k")
    )
    d = d.filter((pl.col("k") >= -n) & (pl.col("k") < n))
    agg = d.group_by("k").agg(
        pl.col("position_value").filter(pl.col("szi") > 0).sum().alias("long_ntl"),
        pl.col("position_value").filter(pl.col("szi") < 0).sum().alias("short_ntl"),
        (pl.col("szi") > 0).sum().alias("n_long"),
        (pl.col("szi") < 0).sum().alias("n_short"),
    )
    grid = pl.DataFrame({"k": list(range(-n, n))}, schema={"k": pl.Int64})
    out = (
        grid.join(agg, on="k", how="left")
        .fill_null(0)
        .with_columns(
            (mark * (1 + pl.col("k") * b)).alias("px_lo"),
            (mark * (1 + (pl.col("k") + 1) * b)).alias("px_hi"),
        )
        .sort("k")
    )
    below = out.filter(pl.col("k") < 0).sort("k", descending=True)
    above = out.filter(pl.col("k") >= 0).sort("k")
    below = below.with_columns(
        pl.col("long_ntl").cum_sum().alias("cum_long"),
        pl.col("short_ntl").cum_sum().alias("cum_short"),
    )
    above = above.with_columns(
        pl.col("long_ntl").cum_sum().alias("cum_long"),
        pl.col("short_ntl").cum_sum().alias("cum_short"),
    )
    return pl.concat([below.sort("k"), above]).select(
        "k", "px_lo", "px_hi", "long_ntl", "n_long", "short_ntl", "n_short", "cum_long", "cum_short"
    )


def net_change(
    conn: sqlite3.Connection,
    coin: str,
    r0: int,
    r1: int,
    w: WalletsConfig,
    data_dir: str | None = None,
) -> tuple[float, int] | None:
    """Change in the watchlist's net position (coin units) between rounds r0 and r1,
    counting only wallets polled successfully in both, so watchlist membership changes
    don't show up as flow. Returns (delta szi, wallets compared)."""
    events = ws.load_events(conn)
    both = ws.active_at(events, r0) & ws.active_at(events, r1)
    failed = {
        r[0]
        for r in conn.execute(
            "SELECT address FROM round_failures WHERE round_ts IN (?, ?)", (r0, r1)
        )
    }
    both -= failed | excluded_addresses(conn, w)
    if not both:
        return None

    def net(r: int) -> float:
        df = round_positions(conn, r, coin, data_dir)
        return float(df.filter(pl.col("address").is_in(list(both)))["szi"].sum())

    return net(r1) - net(r0), len(both)


def load_liquidations(conn: sqlite3.Connection, coin: str, start: int, end: int) -> pl.DataFrame:
    rows = conn.execute(
        """SELECT ts, address, side, sz, px, ntl, method, source FROM liquidations
           WHERE coin = ? AND ts >= ? AND ts < ? ORDER BY ts""",
        (coin, start, end),
    ).fetchall()
    schema = {
        "ts": pl.Int64, "address": pl.Utf8, "side": pl.Utf8, "sz": pl.Float64,
        "px": pl.Float64, "ntl": pl.Float64, "method": pl.Utf8, "source": pl.Utf8,
    }  # fmt: skip
    return pl.DataFrame([tuple(r) for r in rows], schema=schema, orient="row")


def positioning_series(
    conn: sqlite3.Connection,
    coin: str,
    start: int,
    end: int,
    w: WalletsConfig,
    data_dir: str | None = None,
) -> pl.DataFrame:
    """One row per polling round: long/short notional, wallets, coverage."""
    out = []
    for (round_ts,) in conn.execute(
        "SELECT round_ts FROM snapshot_rounds WHERE round_ts >= ? AND round_ts < ? ORDER BY 1",
        (start, end),
    ).fetchall():
        s = summarize(load_positions(conn, coin, round_ts, w, data_dir), round_ts,
                      market_ref(conn, coin, round_ts))  # fmt: skip
        out.append({
            "ts": round_ts, "long_ntl": s.long_ntl, "short_ntl": s.short_ntl,
            "net_ntl": s.net_ntl, "n_long": s.n_long, "n_short": s.n_short,
            "long_cov": s.long_cov, "short_cov": s.short_cov, "combined_cov": s.combined_cov,
            "lev_long": s.lev_long, "lev_short": s.lev_short,
        })  # fmt: skip
    schema = {"ts": pl.Int64, "long_ntl": pl.Float64, "short_ntl": pl.Float64,
              "net_ntl": pl.Float64, "n_long": pl.Int64, "n_short": pl.Int64,
              "long_cov": pl.Float64, "short_cov": pl.Float64, "combined_cov": pl.Float64,
              "lev_long": pl.Float64, "lev_short": pl.Float64}  # fmt: skip
    return pl.DataFrame(out, schema=schema)


def all_rounds(conn: sqlite3.Connection) -> list[int]:
    return [int(r[0]) for r in conn.execute("SELECT round_ts FROM snapshot_rounds ORDER BY 1")]


def positions_by_round(
    conn: sqlite3.Connection, coin: str, rounds: list[int], data_dir: str | None = None
) -> dict[int, dict[str, tuple[float, float]]]:
    """{round: {address: (szi, position_value)}} for many rounds in one pass."""
    out: dict[int, dict[str, tuple[float, float]]] = {r: {} for r in rounds}
    for i in range(0, len(rounds), 500):
        chunk = rounds[i : i + 500]
        marks = ",".join("?" * len(chunk))
        for r in conn.execute(
            f"SELECT round_ts, address, szi, position_value FROM positions"
            f" WHERE coin = ? AND round_ts IN ({marks})",
            (coin, *chunk),
        ):
            out[r[0]][r[1]] = (r[2], r[3])
    if data_dir is not None:
        for r in rounds:
            if not out[r]:
                df = round_positions(conn, r, coin, data_dir)
                out[r] = {
                    a: (s, v)
                    for a, s, v in zip(df["address"], df["szi"], df["position_value"], strict=True)
                }
    return out


def flow_series(
    conn: sqlite3.Connection,
    coin: str,
    rounds: list[int | None],
    w: WalletsConfig,
    data_dir: str | None = None,
) -> list[dict[str, float | int | None]]:
    """Per bar (given the snapshot round at each bar's close, or None): watchlist long and
    short notional, and the same-wallet net change in coin units since the previous
    bar's round (only wallets watched and polled OK in both rounds, none excluded)."""
    known = sorted({r for r in rounds if r is not None})
    snap = positions_by_round(conn, coin, known, data_dir)
    events = ws.load_events(conn)
    excluded = excluded_addresses(conn, w)
    failed: dict[int, set[str]] = {}
    for r in conn.execute("SELECT round_ts, address FROM round_failures"):
        failed.setdefault(r[0], set()).add(r[1])

    def members(r: int) -> set[str]:
        return ws.active_at(events, r) - excluded - failed.get(r, set())

    out: list[dict[str, float | int | None]] = []
    prev: int | None = None
    for r in rounds:
        row: dict[str, float | int | None] = {
            "long_ntl": None, "short_ntl": None, "n_long": None, "n_short": None,
            "net_change": None,
        }  # fmt: skip
        if r is not None:
            pos = {a: v for a, v in snap[r].items() if a not in excluded}
            row["long_ntl"] = sum(v for s, v in pos.values() if s > 0)
            row["short_ntl"] = sum(v for s, v in pos.values() if s < 0)
            row["n_long"] = sum(1 for s, _ in pos.values() if s > 0)
            row["n_short"] = sum(1 for s, _ in pos.values() if s < 0)
            if prev is not None and prev != r:
                both = members(prev) & members(r)
                if both:
                    row["net_change"] = sum(
                        snap[r].get(a, (0.0, 0.0))[0] - snap[prev].get(a, (0.0, 0.0))[0]
                        for a in both
                    )
            prev = r
        out.append(row)
    return out
