"""REST backfill: markets, candles (within the API's depth limit), funding history,
plus a current snapshot of asset contexts and predicted funding.

S3 backfill is deferred (see PLAN.md); anything the REST API can't provide is recorded
as a gap with the reason, so `backfill` reports exactly what couldn't be fetched.
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass, field

from hlflows import coverage as cov
from hlflows import store
from hlflows.api.client import InfoClient
from hlflows.config import Config
from hlflows.data import STORED_RES, bars_stream
from hlflows.markets import Market, resolve_markets, save_markets
from hlflows.timeutil import HOUR_MS, floor_ms, fmt_ms, now_ms

log = logging.getLogger(__name__)

CANDLE_DEPTH = 5000  # candleSnapshot serves only the most recent 5000 candles per interval
FUNDING_PAGE = 500  # fundingHistory returns at most 500 rows per request


@dataclass
class StreamResult:
    market: str
    stream: str
    rows: int = 0
    first_ts: int | None = None
    end_ts: int | None = None
    notes: list[str] = field(default_factory=list)


@dataclass
class BackfillReport:
    results: list[StreamResult] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


async def refresh_markets(
    cfg: Config, conn: sqlite3.Connection, client: InfoClient
) -> tuple[list[Market], list[str]]:
    meta = (await client.meta_and_asset_ctxs(cfg.markets.dex)).meta
    spot = await client.spot_meta()
    markets, warnings = resolve_markets(cfg.markets, meta, spot)
    with store.transaction(conn):
        save_markets(conn, markets)
    return markets, warnings


async def snapshot_context(
    cfg: Config, conn: sqlite3.Connection, client: InfoClient, coins: list[str]
) -> int:
    """Store one REST sample of perp asset contexts and predicted funding. Returns rows."""
    mac = await client.meta_and_asset_ctxs(cfg.markets.dex)
    predicted = await client.predicted_fundings()
    ts = now_ms()
    prefix = f"{cfg.markets.dex}:" if cfg.markets.dex else ""
    wanted = {prefix + c: c for c in coins}
    n = 0
    with store.transaction(conn):
        for asset, ctx in zip(mac.meta.universe, mac.ctxs, strict=True):
            if asset.name in wanted:
                store.upsert_asset_ctx(conn, wanted[asset.name], ts, ctx, "rest")
                n += 1
        # predictedFundings only covers the canonical dex.
        if not cfg.markets.dex:
            n += store.insert_predicted_fundings(conn, ts, predicted, set(coins))
    return n


async def backfill_candles(
    conn: sqlite3.Connection, client: InfoClient, market: Market, res: str, start: int, end: int
) -> StreamResult:
    """Fetch closed candles in [start, end) and mark the range as covered.

    Within the API's depth window a missing candle means no trades, so coverage spans the
    whole requested range, not just first-to-last candle. Anything older than the depth
    window is recorded as a gap (recoverable only from S3).
    """
    res_ms = STORED_RES[res]
    result = StreamResult(market.market, bars_stream(res))
    quality = "low_significance" if market.significance == "low" else None
    now = now_ms()
    end = min(end, now)
    requested = floor_ms(start, res_ms)
    cursor = requested
    while cursor < end:
        candles = await client.candles(market.hl_name, res, cursor, end)
        closed = sorted(
            (c for c in candles if c.t + res_ms <= now and cursor <= c.t < end), key=lambda c: c.t
        )
        if not closed:
            break
        with store.transaction(conn):
            store.upsert_bars(conn, store.bars_from_candles(market.market, res, closed, quality))
        result.rows += len(closed)
        result.first_ts = closed[0].t if result.first_ts is None else result.first_ts
        nxt = closed[-1].t + res_ms
        if nxt <= cursor or nxt + res_ms > end:
            break
        cursor = nxt

    cover_end = floor_ms(end, res_ms)
    depth_start = floor_ms(now, res_ms) - CANDLE_DEPTH * res_ms
    cover_start: int | None
    if requested >= depth_start:
        cover_start = requested
    else:
        cover_start = result.first_ts  # the API served only its most recent candles
        reason = f"beyond candleSnapshot depth ({CANDLE_DEPTH} x {res})"
        hole_end = cover_start if cover_start is not None else cover_end
        with store.transaction(conn):
            cov.record_gap(conn, result.stream, market.market, requested, hole_end, reason, "s3")
        result.notes.append(f"{fmt_ms(requested)} → {fmt_ms(hole_end)}: {reason}")
    with store.transaction(conn):
        if cover_start is not None and cover_end > cover_start:
            cov.add_coverage(conn, result.stream, market.market, cover_start, cover_end)
            result.end_ts = cover_end
        cov.close_filled_gaps(conn, result.stream, market.market)
    return result


async def backfill_funding(
    conn: sqlite3.Connection, client: InfoClient, coin: str, start: int, end: int
) -> StreamResult:
    result = StreamResult(coin, "funding")
    cursor = start
    hours: list[int] = []
    while cursor < end:
        records = await client.funding_history(coin, cursor, end)
        records = [r for r in records if cursor <= r.time < end]
        if not records:
            break
        with store.transaction(conn):
            store.upsert_funding(conn, records)
        hours.extend(floor_ms(r.time, HOUR_MS) for r in records)
        result.rows += len(records)
        if len(records) < FUNDING_PAGE:
            break
        nxt = records[-1].time + 1
        if nxt <= cursor:
            break
        cursor = nxt
    if not hours:
        result.notes.append("no funding records returned (coin not listed in range?)")
        return result

    hours = sorted(set(hours))
    result.first_ts, result.end_ts = hours[0], hours[-1] + HOUR_MS
    with store.transaction(conn):
        for run_start, run_end in _runs(hours, HOUR_MS):
            cov.add_coverage(conn, "funding", coin, run_start, run_end)
        present = set(hours)
        missing = [h for h in range(hours[0], hours[-1], HOUR_MS) if h not in present]
        for span_start, span_end in _runs(missing, HOUR_MS):
            cov.record_gap(
                conn,
                "funding",
                coin,
                span_start,
                span_end,
                "hour missing from fundingHistory",
                "rest",
            )
            result.notes.append(f"missing {fmt_ms(span_start)} → {fmt_ms(span_end)}")
        cov.close_filled_gaps(conn, "funding", coin)
    if hours[0] > floor_ms(start, HOUR_MS) + HOUR_MS:
        result.notes.append(f"history starts {fmt_ms(hours[0])} (listing date or API limit)")
    return result


def _runs(points: list[int], step: int) -> list[cov.Interval]:
    """Group sorted timestamps into contiguous [start, end) runs."""
    runs: list[cov.Interval] = []
    for p in points:
        if runs and runs[-1][1] == p:
            runs[-1] = (runs[-1][0], p + step)
        else:
            runs.append((p, p + step))
    return runs


async def run_backfill(
    cfg: Config,
    conn: sqlite3.Connection,
    client: InfoClient,
    coins: list[str],
    start: int,
    end: int | None = None,
) -> BackfillReport:
    end = now_ms() if end is None else end
    report = BackfillReport()
    markets, warnings = await refresh_markets(cfg, conn, client)
    report.warnings.extend(warnings)
    for coin in coins:
        coin_markets = [m for m in markets if m.coin == coin]
        if not coin_markets:
            report.warnings.append(f"{coin}: no markets resolved")
            continue
        for market in coin_markets:
            for res in ("1d", "1h", "1m"):
                log.info("candles %s %s", market.market, res)
                report.results.append(await backfill_candles(conn, client, market, res, start, end))
            if market.kind == "perp":
                log.info("funding %s", coin)
                report.results.append(await backfill_funding(conn, client, coin, start, end))
    await snapshot_context(cfg, conn, client, coins)
    return report
