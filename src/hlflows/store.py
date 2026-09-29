"""Idempotent writes. Re-running any write with the same input leaves the same rows."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass

from hlflows.api import types as t
from hlflows.timeutil import HOUR_MS, MINUTE_MS, floor_ms


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    conn.execute("COMMIT")


@dataclass(frozen=True)
class Bar:
    market: str
    res: str
    ts: int
    o: float
    h: float
    l: float  # noqa: E741
    c: float
    volume: float
    n: int
    buy_sz: float | None
    sell_sz: float | None
    buy_ntl: float | None
    sell_ntl: float | None
    source: str
    quality: str | None


def bars_from_candles(
    market: str, res: str, candles: Iterable[t.Candle], quality: str | None
) -> list[Bar]:
    return [
        Bar(
            market,
            res,
            c.t,
            c.o,
            c.h,
            c.l,
            c.c,
            c.v,
            c.n,
            None,
            None,
            None,
            None,
            "candle",
            quality,
        )
        for c in candles
    ]


def upsert_bars(conn: sqlite3.Connection, bars: Sequence[Bar]) -> None:
    """Insert or replace bars. Recorded ('ws') bars carry the taker split and are never
    overwritten by candle or S3 data, unless the recorded bar is partial (the recorder
    wasn't connected for the whole minute). Anything may be overwritten by a 'ws' bar."""
    conn.executemany(
        """INSERT INTO bars (market, res, ts, o, h, l, c, volume, n, buy_sz, sell_sz,
                             buy_ntl, sell_ntl, source, quality)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT (market, res, ts) DO UPDATE SET
             o = excluded.o, h = excluded.h, l = excluded.l, c = excluded.c,
             volume = excluded.volume, n = excluded.n,
             buy_sz = excluded.buy_sz, sell_sz = excluded.sell_sz,
             buy_ntl = excluded.buy_ntl, sell_ntl = excluded.sell_ntl,
             source = excluded.source, quality = excluded.quality
           WHERE bars.source <> 'ws' OR excluded.source = 'ws'
              OR (bars.quality IS NOT NULL AND bars.quality LIKE '%partial%')""",
        [
            (
                b.market,
                b.res,
                b.ts,
                b.o,
                b.h,
                b.l,
                b.c,
                b.volume,
                b.n,
                b.buy_sz,
                b.sell_sz,
                b.buy_ntl,
                b.sell_ntl,
                b.source,
                b.quality,
            )
            for b in bars
        ],
    )


def upsert_funding(conn: sqlite3.Connection, records: Iterable[t.FundingRecord]) -> int:
    rows = [(r.coin, floor_ms(r.time, HOUR_MS), r.fundingRate, r.premium, r.time) for r in records]
    conn.executemany(
        """INSERT INTO funding (coin, ts, rate_1h, premium, raw_time) VALUES (?, ?, ?, ?, ?)
           ON CONFLICT (coin, ts) DO UPDATE SET rate_1h = excluded.rate_1h,
             premium = excluded.premium, raw_time = excluded.raw_time""",
        rows,
    )
    return len(rows)


def upsert_asset_ctx(
    conn: sqlite3.Connection, coin: str, ts: int, ctx: t.PerpAssetCtx, source: str
) -> None:
    """One sample per minute (the last one seen). A recorded ('ws') sample is never
    replaced by a REST sample for the same minute."""
    bid, ask = (ctx.impactPxs[0], ctx.impactPxs[1]) if ctx.impactPxs else (None, None)
    conn.execute(
        """INSERT INTO asset_ctx (coin, ts, oi, mark_px, oracle_px, mid_px, premium, funding_1h,
                                  impact_bid, impact_ask, day_ntl_vlm, source, quality)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
           ON CONFLICT (coin, ts) DO UPDATE SET oi = excluded.oi, mark_px = excluded.mark_px,
             oracle_px = excluded.oracle_px, mid_px = excluded.mid_px,
             premium = excluded.premium, funding_1h = excluded.funding_1h,
             impact_bid = excluded.impact_bid, impact_ask = excluded.impact_ask,
             day_ntl_vlm = excluded.day_ntl_vlm, source = excluded.source
           WHERE asset_ctx.source <> 'ws' OR excluded.source = 'ws'""",
        (
            coin,
            floor_ms(ts, MINUTE_MS),
            ctx.openInterest,
            ctx.markPx,
            ctx.oraclePx,
            ctx.midPx,
            ctx.premium,
            ctx.funding,
            bid,
            ask,
            ctx.dayNtlVlm,
            source,
        ),
    )


def insert_predicted_fundings(
    conn: sqlite3.Connection, observed_ts: int, data: t.PredictedFundings, coins: set[str]
) -> int:
    rows = [
        (coin, venue, observed_ts, vf.fundingRate, vf.fundingIntervalHours, vf.nextFundingTime)
        for coin, venues in data
        if coin in coins
        for venue, vf in venues
        if vf is not None
    ]
    conn.executemany(
        """INSERT INTO predicted_funding (coin, venue, observed_ts, rate, interval_h, next_ts)
           VALUES (?, ?, ?, ?, ?, ?)
           ON CONFLICT (coin, venue, observed_ts) DO UPDATE SET rate = excluded.rate,
             interval_h = excluded.interval_h, next_ts = excluded.next_ts""",
        rows,
    )
    return len(rows)
