"""`dashboard`: one self-contained HTML page of the flows data for every configured coin.

Python gathers the series (through the same query layer as `show`); the page draws them
with Plotly as stacked panels sharing one time axis. Each measure has its own panel and
scale (no dual axes). Colour meaning is fixed across the page: blue = buy / long / up,
red = sell / short / down.
"""

from __future__ import annotations

import bisect
import json
import math
import sqlite3
from pathlib import Path
from typing import Any

import polars as pl

from hlflows import data
from hlflows import show as show_mod
from hlflows.config import Config
from hlflows.markets import spot_market_id
from hlflows.timeutil import (
    TIMEFRAMES,
    Timeframe,
    bucket_start,
    fmt_ms,
    next_bucket_start,
    now_ms,
)
from hlflows.wallets import positioning as pos

DEFAULT_TFS = ("15m", "1h", "4h", "1d")
SUMMARY_TFS = ("15m", "1h", "4h", "1d", "1w")


def _utc(ms: int) -> str:
    return fmt_ms(ms) + ":00"


def _window_start(tf: Timeframe, n_bars: int, now: int, week_start: str) -> int:
    b = bucket_start(now, tf, week_start)
    for _ in range(n_bars - 1):
        b = bucket_start(b - 1, tf, week_start)
    return b


def _cvd(deltas: list[float | None]) -> list[float | None]:
    """Cumulative delta anchored at the window start. Bars without a taker split stay
    empty (a break in the line); the sum resumes from its last value after them."""
    out: list[float | None] = []
    total = 0.0
    for d in deltas:
        if d is None:
            out.append(None)
        else:
            total += d
            out.append(total)
    return out


def _round_at_close(rounds: list[int], bar_end: int, slack: int) -> int | None:
    i = bisect.bisect_left(rounds, bar_end) - 1
    if i < 0 or bar_end - rounds[i] > slack:
        return None
    return rounds[i]


def build_timeframe(
    cfg: Config, conn: sqlite3.Connection, coin: str, tf: Timeframe, n_bars: int
) -> dict[str, Any]:
    ws = cfg.time.week_start
    now = now_ms()
    start = _window_start(tf, n_bars, now, ws)
    bars = data.load_bars(conn, coin, tf, start, now, ws)
    if bars.is_empty():
        return {"ts": []}
    ts = bars["ts"].to_list()
    spot = data.load_bars(conn, spot_market_id(coin), tf, start, now, ws)
    spot_delta = dict(zip(spot["ts"].to_list(), spot["delta"].to_list(), strict=True))
    spot_cov = dict(zip(spot["ts"].to_list(), spot["taker_cov"].to_list(), strict=True))
    ctx = data.load_ctx(conn, coin, tf, start, now, ws)
    oi = dict(zip(ctx["ts"].to_list(), ctx["oi_close"].to_list(), strict=True))
    fb = data.load_funding_bars(conn, coin, tf, start, now, ws)
    fund = {r["ts"]: r for r in fb.iter_rows(named=True)}

    ends = [next_bucket_start(t, tf) for t in ts]
    liqs = pos.load_liquidations(conn, coin, start, ends[-1])
    liq_long = [0.0] * len(ts)
    liq_short = [0.0] * len(ts)
    for r in liqs.iter_rows(named=True):
        i = bisect.bisect_right(ts, r["ts"]) - 1
        if 0 <= i < len(ts):
            (liq_long if r["side"] == "long" else liq_short)[i] += r["ntl"]

    rounds = pos.all_rounds(conn)
    slack = int(2 * cfg.wallets.poll_interval_s * 1000)
    at_close = [_round_at_close(rounds, min(e, now), slack) for e in ends]
    flows = pos.flow_series(conn, coin, at_close, cfg.wallets, cfg.storage.data_dir)
    closes = bars["c"].to_list()
    net_flow_usd: list[float | None] = []
    running = 0.0
    for f, c in zip(flows, closes, strict=True):
        change = f["net_change"]
        if change is None:
            net_flow_usd.append(None if f["long_ntl"] is None else running)
        else:
            running += float(change) * c
            net_flow_usd.append(running)

    def pct(x: float | None, scale: float = 100.0) -> float | None:
        return None if x is None else x * scale

    return {
        "ts": [_utc(t) for t in ts],
        "closed": bars["closed"].to_list(),
        "o": bars["o"].to_list(),
        "h": bars["h"].to_list(),
        "l": bars["l"].to_list(),
        "c": closes,
        "volume": bars["volume"].to_list(),
        "delta": bars["delta"].to_list(),
        "taker_cov": bars["taker_cov"].to_list(),
        "spot_taker_cov": [spot_cov.get(t) for t in ts],
        "cvd": _cvd(bars["delta"].to_list()),
        "spot_cvd": _cvd([spot_delta.get(t) for t in ts]),
        "oi": [oi.get(t) for t in ts],
        "funding_8h": [pct(fund.get(t, {}).get("rate_8h")) for t in ts],
        "premium": [pct(fund.get(t, {}).get("premium")) for t in ts],
        "watch_long": [f["long_ntl"] for f in flows],
        "watch_short": [f["short_ntl"] for f in flows],
        "watch_flow": net_flow_usd,
        "liq_long": liq_long,
        "liq_short": liq_short,
        "res": bars["res"].to_list(),
    }


def build_coin(
    cfg: Config, conn: sqlite3.Connection, coin: str, tfs: list[Timeframe], n_bars: int
) -> dict[str, Any]:
    summary = []
    for name in SUMMARY_TFS:
        r = show_mod.build_row(cfg, conn, coin, TIMEFRAMES[name])
        summary.append(r.__dict__ | {"bar": fmt_ms(r.bar_ts) if r.bar_ts else None})

    stats = data.current_funding_stats(conn, coin, cfg.funding.percentile_window_days)
    venues = [v.__dict__ for v in data.latest_predicted_funding(conn, coin)]

    positioning: dict[str, Any] | None = None
    liqmap: dict[str, Any] | None = None
    round_ts = pos.latest_round(conn)
    if round_ts is not None:
        ref = pos.market_ref(conn, coin, round_ts)
        df = pos.load_positions(conn, coin, round_ts, cfg.wallets, cfg.storage.data_dir)
        s = pos.summarize(df, round_ts, ref)
        positioning = s.__dict__ | {"net_ntl": s.net_ntl, "snapshot": fmt_ms(round_ts)}
        if ref is not None and not df.is_empty():
            m = pos.liqmap(df, ref.mark, cfg.wallets.liqmap_bucket_pct,
                           cfg.wallets.liqmap_range_pct)  # fmt: skip
            m = m.with_columns(((pl.col("px_lo") + pl.col("px_hi")) / 2).alias("px"))
            liqmap = {
                "mark": ref.mark,
                "px": m["px"].to_list(),
                "long": m["long_ntl"].to_list(),
                "short": m["short_ntl"].to_list(),
                "n_long": m["n_long"].to_list(),
                "n_short": m["n_short"].to_list(),
            }
    return {
        "coin": coin,
        "summary": summary,
        "funding": None if stats is None else stats.__dict__ | {"at": fmt_ms(stats.ts)},
        "venues": venues,
        "positioning": positioning,
        "liqmap": liqmap,
        "tfs": {tf.name: build_timeframe(cfg, conn, coin, tf, n_bars) for tf in tfs},
    }


def recorder_line(conn: sqlite3.Connection) -> str:
    row = conn.execute("SELECT * FROM stream_state WHERE stream = 'recorder'").fetchone()
    if row is None:
        return "recorder never run on this database"
    age = (now_ms() - row["updated_ts"]) / 1000
    state = row["detail"] if age < 60 else f"no heartbeat for {age / 60:.0f} min"
    return f"recorder {state}"


def build(
    cfg: Config, conn: sqlite3.Connection, coins: list[str], tfs: list[Timeframe], n_bars: int
) -> dict[str, Any]:
    return {
        "generated": fmt_ms(now_ms()),
        "recorder": recorder_line(conn),
        "tfs": [tf.name for tf in tfs],
        "coins": {c: build_coin(cfg, conn, c, tfs, n_bars) for c in coins},
        "coverage_note": (
            f"Watchlist = up to {cfg.wallets.max_active} wallets found in the trades feed "
            "(vaults and market makers excluded). Coverage = watchlist long (or short) "
            "notional / open interest."
        ),
    }


def _clean(obj: Any) -> Any:
    """JSON-safe: NaN/inf become null (JSON has no NaN)."""
    if isinstance(obj, float):
        return obj if math.isfinite(obj) else None
    if isinstance(obj, dict):
        return {k: _clean(v) for k, v in obj.items()}
    if isinstance(obj, list | tuple):
        return [_clean(v) for v in obj]
    return obj


def render_html(payload: dict[str, Any]) -> str:
    template = (Path(__file__).parent / "dashboard_template.html").read_text()
    blob = json.dumps(_clean(payload), separators=(",", ":"), allow_nan=False, default=str)
    return template.replace("/*__DATA__*/null", blob.replace("</", "<\\/"))
