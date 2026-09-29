"""`liqmap`: the watchlist's liquidation map around the current mark."""

from __future__ import annotations

import sqlite3

import polars as pl
from rich.console import Group, RenderableType
from rich.table import Table
from rich.text import Text

from hlflows.config import Config
from hlflows.timeutil import fmt_ms, now_ms
from hlflows.wallets import positioning as pos

BAR_WIDTH = 24


def usd(v: float | None) -> str:
    if v is None:
        return "—"
    a = abs(v)
    for div, suffix in ((1e9, "B"), (1e6, "M"), (1e3, "k")):
        if a >= div:
            return f"{v / div:,.1f}{suffix}"
    return f"{v:,.0f}"


def pct(v: float | None) -> str:
    return "—" if v is None else f"{v * 100:.1f}%"


def _bar(v: float, vmax: float, style: str) -> Text:
    n = round(BAR_WIDTH * v / vmax) if vmax > 0 else 0
    return Text("█" * n, style=style)


def coverage_line(s: pos.Summary, ref: pos.MarketRef | None) -> str:
    oi = f"OI {usd(ref.oi_ntl)}" if ref else "OI unknown (no recorded context)"
    return (
        f"coverage: long {pct(s.long_cov)}, short {pct(s.short_cov)}, "
        f"combined {pct(s.combined_cov)} of {oi}"
    )


def render(
    cfg: Config,
    conn: sqlite3.Connection,
    coin: str,
    bucket_pct: float,
    range_pct: float,
    at: int | None = None,
) -> RenderableType:
    round_ts = pos.latest_round(conn, at)
    if round_ts is None:
        return Text(
            "No positioning snapshots yet: the recorder polls the watchlist.", style="yellow"
        )
    df = pos.load_positions(conn, coin, round_ts, cfg.wallets, cfg.storage.data_dir)
    ref = pos.market_ref(conn, coin, round_ts)
    mark = ref.mark if ref else None
    if mark is None and df.height:
        mark = float((df["position_value"] / df["szi"].abs()).median())  # type: ignore[arg-type]
    s = pos.summarize(df, round_ts, ref)
    now = now_ms()
    header = Text(
        f"{coin} liquidation map — watchlist snapshot {fmt_ms(round_ts)} UTC "
        f"({(now - round_ts) / 60000:.0f} min ago), mark {mark:,.6g}\n"
        if mark
        else "",
        style="bold",
    )
    header.append(
        f"{s.n_long} long / {s.n_short} short positions, long {usd(s.long_ntl)}, "
        f"short {usd(s.short_ntl)}; {coverage_line(s, ref)}\n"
    )
    header.append(
        f"without a liquidation price: {usd(s.no_liq_ntl)} · cross-margin liquidation prices "
        "move with the account and are a point-in-time estimate",
        style="dim",
    )
    if mark is None or df.is_empty():
        return Group(header, Text(f"No watched positions in {coin}.", style="yellow"))

    m = pos.liqmap(df, mark, bucket_pct, range_pct)
    vmax = max(float(m["long_ntl"].max() or 0), float(m["short_ntl"].max() or 0))  # type: ignore[arg-type]
    table = Table(title_justify="left", show_edge=False)
    for col, just in (("liq price", "right"), ("shorts liquidated ↑", "left"), ("short", "right"),
                      ("cum short", "right"), ("longs liquidated ↓", "left"), ("long", "right"),
                      ("cum long", "right")):  # fmt: skip
        table.add_column(col, justify=just)  # type: ignore[arg-type]
    rows = m.sort("k", descending=True).iter_rows(named=True)
    marked = False
    for r in rows:
        if r["k"] < 0 and not marked:
            table.add_row(Text(f"── mark {mark:,.6g} ──", style="bold"), "", "", "", "", "", "")
            marked = True
        if r["long_ntl"] == 0 and r["short_ntl"] == 0:
            continue
        table.add_row(
            f"{r['px_lo']:,.6g} - {r['px_hi']:,.6g}",
            _bar(r["short_ntl"], vmax, "red"),
            usd(r["short_ntl"]) if r["short_ntl"] else "",
            usd(r["cum_short"]) if r["k"] >= 0 else "",
            _bar(r["long_ntl"], vmax, "green"),
            usd(r["long_ntl"]) if r["long_ntl"] else "",
            usd(r["cum_long"]) if r["k"] < 0 else "",
        )
    lev = pos.leverage_distribution(df)
    lt = Table(title="Effective leverage (notional)", title_justify="left")
    for col in ("leverage", "long", "# long", "short", "# short"):
        lt.add_column(col, justify="left" if col == "leverage" else "right")
    for r in lev.iter_rows(named=True):
        lt.add_row(r["bucket"], usd(r["long_ntl"]), str(r["n_long"]), usd(r["short_ntl"]),
                   str(r["n_short"]))  # fmt: skip
    return Group(header, table, lt)


def liqmap_frame(
    cfg: Config, conn: sqlite3.Connection, coin: str, at: int | None = None
) -> pl.DataFrame:
    """The liquidation map as a DataFrame (for export)."""
    round_ts = pos.latest_round(conn, at)
    if round_ts is None:
        return pl.DataFrame()
    df = pos.load_positions(conn, coin, round_ts, cfg.wallets, cfg.storage.data_dir)
    ref = pos.market_ref(conn, coin, round_ts)
    if ref is None or df.is_empty():
        return pl.DataFrame()
    return pos.liqmap(df, ref.mark, cfg.wallets.liqmap_bucket_pct, cfg.wallets.liqmap_range_pct)
