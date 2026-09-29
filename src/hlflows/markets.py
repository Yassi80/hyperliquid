"""Resolve configured coins to Hyperliquid perp and spot markets."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from hlflows.api import types as t
from hlflows.config import MarketsConfig
from hlflows.timeutil import now_ms


@dataclass(frozen=True)
class Market:
    market: str  # hlflows id: 'BTC' (perp) or 'BTC/spot'
    coin: str
    kind: str  # 'perp' | 'spot'
    hl_name: str  # name used in API requests
    significance: str  # 'normal' | 'low'


def spot_market_id(coin: str) -> str:
    return f"{coin}/spot"


def resolve_markets(
    cfg: MarketsConfig, perp_meta: t.PerpMeta, spot_meta: t.SpotMeta
) -> tuple[list[Market], list[str]]:
    """Return the markets to track plus human-readable warnings for anything unresolved."""
    warnings: list[str] = []
    markets: list[Market] = []
    perp_names = {a.name for a in perp_meta.universe if not a.isDelisted}
    prefix = f"{cfg.dex}:" if cfg.dex else ""
    for coin in cfg.coins:
        name = prefix + coin
        if name in perp_names:
            markets.append(Market(coin, coin, "perp", name, "normal"))
        else:
            warnings.append(f"perp {name!r} not found on dex {cfg.dex or '(canonical)'}")

    tokens = {tok.index: tok.name for tok in spot_meta.tokens}
    pairs = {
        (tokens.get(p.tokens[0]), tokens.get(p.tokens[1])): p
        for p in spot_meta.universe
        if len(p.tokens) == 2
    }
    for coin, base in cfg.spot_tokens.items():
        pair = pairs.get((base, cfg.spot_quote))
        if pair is None:
            warnings.append(f"spot {base}/{cfg.spot_quote} not found for {coin}")
            continue
        significance = "normal" if coin in cfg.spot_primary else "low"
        markets.append(Market(spot_market_id(coin), coin, "spot", pair.name, significance))
    return markets, warnings


def save_markets(conn: sqlite3.Connection, markets: list[Market]) -> None:
    ts = now_ms()
    conn.executemany(
        """INSERT INTO markets (market, coin, kind, hl_name, significance, updated_ts)
           VALUES (?, ?, ?, ?, ?, ?)
           ON CONFLICT (market) DO UPDATE SET coin = excluded.coin, kind = excluded.kind,
             hl_name = excluded.hl_name, significance = excluded.significance,
             updated_ts = excluded.updated_ts""",
        [(m.market, m.coin, m.kind, m.hl_name, m.significance, ts) for m in markets],
    )


def load_markets(conn: sqlite3.Connection, coin: str | None = None) -> list[Market]:
    sql = "SELECT market, coin, kind, hl_name, significance FROM markets"
    args: tuple[str, ...] = ()
    if coin is not None:
        sql += " WHERE coin = ?"
        args = (coin,)
    rows = conn.execute(sql + " ORDER BY kind, market", args).fetchall()
    return [Market(*tuple(r)) for r in rows]
