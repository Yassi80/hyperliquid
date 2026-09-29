"""`show`: per-timeframe flow summary for one coin."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

import polars as pl
from rich.console import Group, RenderableType
from rich.table import Table
from rich.text import Text

from hlflows import data
from hlflows.config import Config
from hlflows.liqmap import coverage_line, usd
from hlflows.markets import spot_market_id
from hlflows.metrics import cvd_slope, pct_change
from hlflows.timeutil import Timeframe, bucket_start, fmt_ms, next_bucket_start, now_ms
from hlflows.wallets import positioning as pos

CVD_SLOPE_HELP = (
    "CVD slope = least-squares slope of cumulative delta over the last N closed bars, "
    "divided by the mean bar taker volume (so +1 = CVD rising by one average bar's volume "
    "per bar). N is show.cvd_slope_bars in the config."
)


@dataclass(frozen=True)
class TfRow:
    tf: str
    bar_ts: int | None = None
    close: float | None = None
    price_chg_pct: float | None = None
    oi_close: float | None = None
    oi_chg_pct: float | None = None
    delta: float | None = None
    cvd_slope: float | None = None
    spot_delta: float | None = None
    spot_quality: str | None = None
    funding_1h: float | None = None
    funding_8h: float | None = None
    premium: float | None = None
    source_res: str | None = None
    complete: bool = False
    watch_dnet_usd: float | None = None  # same-wallet net position change over the bar
    liq_long: float = 0.0
    liq_short: float = 0.0


def _window_start(tf: Timeframe, n_bars: int, now: int, week_start: str) -> int:
    b = bucket_start(now, tf, week_start)
    for _ in range(n_bars):
        b = bucket_start(b - 1, tf, week_start)
    return b


def _closed(df: pl.DataFrame) -> pl.DataFrame:
    return df.filter(pl.col("closed")) if not df.is_empty() else df


def build_row(cfg: Config, conn: sqlite3.Connection, coin: str, tf: Timeframe) -> TfRow:
    now = now_ms()
    ws = cfg.time.week_start
    n = cfg.show.cvd_slope_bars
    start = _window_start(tf, n + 1, now, ws)

    bars = _closed(data.load_bars(conn, coin, tf, start, now, ws))
    if bars.is_empty():
        return TfRow(tf.name)
    last = bars.row(-1, named=True)
    prev = bars.row(-2, named=True) if bars.height >= 2 else None

    ctx = data.load_ctx(conn, coin, tf, start, now, ws)
    ctx_by_ts = {r["ts"]: r for r in ctx.iter_rows(named=True)}
    oi_last = ctx_by_ts.get(last["ts"], {}).get("oi_close")
    oi_prev = ctx_by_ts.get(prev["ts"], {}).get("oi_close") if prev else None

    window = bars.tail(n)
    volumes = [
        (b + s) if b is not None and s is not None else None
        for b, s in zip(window["buy_sz"].to_list(), window["sell_sz"].to_list(), strict=True)
    ]

    spot = _closed(data.load_bars(conn, spot_market_id(coin), tf, last["ts"], now, ws))
    spot_last = spot.filter(pl.col("ts") == last["ts"])
    spot_delta = spot_last["delta"][0] if not spot_last.is_empty() else None
    spot_quality = spot_last["quality"][0] if not spot_last.is_empty() else None

    bar_end = next_bucket_start(last["ts"], tf)
    liqs = pos.load_liquidations(conn, coin, last["ts"], bar_end)
    liq_long = float(liqs.filter(pl.col("side") == "long")["ntl"].sum())
    liq_short = float(liqs.filter(pl.col("side") == "short")["ntl"].sum())
    watch_dnet_usd = None
    slack = int(2 * cfg.wallets.poll_interval_s * 1000)
    r0, r1 = pos.latest_round(conn, last["ts"]), pos.latest_round(conn, bar_end - 1)
    if (
        r0 is not None
        and r1 is not None
        and r0 != r1
        and last["ts"] - r0 <= slack
        and bar_end - r1 <= slack
    ):
        change = pos.net_change(conn, coin, r0, r1, cfg.wallets, cfg.storage.data_dir)
        if change is not None:
            watch_dnet_usd = change[0] * last["c"]

    fb = data.load_funding_bars(conn, coin, tf, last["ts"], now, ws)
    f_last = fb.filter(pl.col("ts") == last["ts"])
    f = f_last.row(0, named=True) if not f_last.is_empty() else {}

    return TfRow(
        tf=tf.name,
        bar_ts=last["ts"],
        close=last["c"],
        price_chg_pct=pct_change(prev["c"], last["c"]) if prev else None,
        oi_close=oi_last,
        oi_chg_pct=pct_change(oi_prev, oi_last),
        delta=last["delta"],
        cvd_slope=cvd_slope(window["delta"].to_list(), volumes),
        spot_delta=spot_delta,
        spot_quality=spot_quality,
        funding_1h=f.get("rate_1h"),
        funding_8h=f.get("rate_8h"),
        premium=f.get("premium"),
        source_res=last["res"],
        complete=bool(last["complete"]),
        watch_dnet_usd=watch_dnet_usd,
        liq_long=liq_long,
        liq_short=liq_short,
    )


def _num(v: float | None, fmt: str, suffix: str = "") -> str:
    return "—" if v is None else f"{v:{fmt}}{suffix}"


def _pct(v: float | None, digits: int = 2, scale: float = 1.0) -> str:
    return "—" if v is None else f"{v * scale:+.{digits}f}%"


def render(
    cfg: Config, conn: sqlite3.Connection, coin: str, tfs: list[Timeframe]
) -> RenderableType:
    table = Table(title=f"{coin} — last closed bar per timeframe (UTC)", title_justify="left")
    for col in (
        "tf",
        "bar open",
        "close",
        "Δ price",
        "OI (coin)",
        "Δ OI",
        "delta",
        "CVD slope",
        "spot delta",
        "fund 1h",
        "fund 8h",
        "premium 8h",
        "watch Δnet",
        "liqs L/S",
        "src",
    ):
        table.add_column(col, justify="right" if col not in ("tf", "bar open", "src") else "left")
    rows = [build_row(cfg, conn, coin, tf) for tf in tfs]
    for r in rows:
        spot = _num(r.spot_delta, ",.2f")
        if r.spot_delta is not None and r.spot_quality and "low_significance" in r.spot_quality:
            spot += " (low sig.)"
        table.add_row(
            r.tf,
            fmt_ms(r.bar_ts) if r.bar_ts else "—",
            _num(r.close, ",.6g"),
            _pct(r.price_chg_pct),
            _num(r.oi_close, ",.0f"),
            _pct(r.oi_chg_pct),
            _num(r.delta, ",.2f"),
            _num(r.cvd_slope, "+.2f"),
            spot,
            _pct(r.funding_1h, 5, 100),
            _pct(r.funding_8h, 4, 100),
            _pct(r.premium, 4, 100),
            ("+" if (r.watch_dnet_usd or 0) > 0 else "") + usd(r.watch_dnet_usd),
            f"{usd(r.liq_long)}/{usd(r.liq_short)}" if r.liq_long or r.liq_short else "—",
            (r.source_res or "—") + ("" if r.complete or r.source_res is None else " partial"),
        )

    parts: list[RenderableType] = [table]
    if all(r.bar_ts is None for r in rows):
        parts.append(
            Text(f"No bars for {coin} yet: run `hlflows backfill --coin {coin} --from <date>`.")
        )
    round_ts = pos.latest_round(conn)
    if round_ts is not None:
        ref = pos.market_ref(conn, coin, round_ts)
        sm = pos.summarize(
            pos.load_positions(conn, coin, round_ts, cfg.wallets, cfg.storage.data_dir),
            round_ts,
            ref,
        )
        pt = Table(
            title=f"Watchlist positioning — snapshot {fmt_ms(round_ts)} "
            f"({(now_ms() - round_ts) / 60000:.0f} min ago); {coverage_line(sm, ref)}",
            title_justify="left",
        )
        for col in ("long", "short", "net", "# long", "# short", "eff lev L", "eff lev S",
                    "no liq px"):  # fmt: skip
            pt.add_column(col, justify="right")
        pt.add_row(
            usd(sm.long_ntl), usd(sm.short_ntl), usd(sm.net_ntl), str(sm.n_long), str(sm.n_short),
            _num(sm.lev_long, ".1f", "x"), _num(sm.lev_short, ".1f", "x"), usd(sm.no_liq_ntl),
        )  # fmt: skip
        parts.append(pt)

    stats = data.current_funding_stats(conn, coin, cfg.funding.percentile_window_days)
    if stats is not None:
        f = Table(
            title=f"Funding — latest settled hour {fmt_ms(stats.ts)}, "
            f"percentiles vs trailing {cfg.funding.percentile_window_days}d ({stats.window_n} h)",
            title_justify="left",
        )
        for col in ("1h", "8h equiv", "premium (8h)", "premium pct", "funding pct", "pinned"):
            f.add_column(col, justify="right")
        f.add_row(
            _pct(stats.rate_1h, 5, 100),
            _pct(stats.rate_8h, 4, 100),
            _pct(stats.premium, 4, 100),
            _num(stats.premium_pct, ".0f"),
            _num(stats.funding_pct, ".0f"),
            _num(None if stats.pinned_share is None else stats.pinned_share * 100, ".0f", "%"),
        )
        parts.append(f)

    venues = data.latest_predicted_funding(conn, coin)
    if venues:
        v = Table(
            title=f"Predicted next funding by venue (observed {fmt_ms(venues[0].observed_ts)})",
            title_justify="left",
        )
        for col in ("venue", "rate", "interval", "8h equiv", "next"):
            v.add_column(col, justify="left" if col == "venue" else "right")
        for row in venues:
            v.add_row(
                row.venue,
                _pct(row.rate, 5, 100),
                f"{row.interval_h:g}h",
                _pct(row.rate_8h, 4, 100),
                fmt_ms(row.next_ts),
            )
        parts.append(v)

    parts.append(
        Text(
            "delta/CVD exist only for minutes the recorder was live (bars from candles have "
            "none); OI change needs recorded context. Premium is the 8h-scale index; funding "
            "pins at +0.00125%/h when it is between -0.04% and +0.06%. watch Δnet: change in "
            "the watchlist's net position over the bar (same wallets at both ends, USD at "
            "close). liqs: liquidations seen (watched wallets + backstop), partial.",
            style="dim",
        )
    )
    return Group(*parts)
