"""Wallet tables: address activity, probe queue, watchlist membership, positions, liquidations."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass

from hlflows.api import types as t
from hlflows.timeutil import HOUR_MS

REPROBE_LARGE_AFTER_MS = HOUR_MS  # a new large trade re-queues an address probed before this


@dataclass
class AddressActivity:
    first_ts: int
    last_ts: int
    n: int = 0
    taker_ntl: float = 0.0
    maker_ntl: float = 0.0
    max_ntl: float = 0.0
    large: bool = False


def aggregate_activity(
    trades: Iterable[tuple[str, t.WsTrade]],
    coin_of_market: Mapping[str, str],
    large_ntl: Mapping[str, float],
) -> dict[str, AddressActivity]:
    """Per-address fill counts and notional from newly seen trades. The taker is the
    buyer when the taker side is B, else the seller."""
    out: dict[str, AddressActivity] = {}
    for market, tr in trades:
        ntl = tr.px * tr.sz
        coin = coin_of_market.get(market, market)
        large = ntl >= large_ntl.get(coin, float("inf"))
        buyer, seller = tr.users
        taker, maker = (buyer, seller) if tr.side == "B" else (seller, buyer)
        for addr, is_taker in ((taker, True), (maker, False)):
            a = out.get(addr)
            if a is None:
                a = out[addr] = AddressActivity(tr.time, tr.time)
            a.first_ts = min(a.first_ts, tr.time)
            a.last_ts = max(a.last_ts, tr.time)
            a.n += 1
            if is_taker:
                a.taker_ntl += ntl
            else:
                a.maker_ntl += ntl
            a.max_ntl = max(a.max_ntl, ntl)
            a.large = a.large or large
    return out


def upsert_activity(
    conn: sqlite3.Connection, activity: Mapping[str, AddressActivity], now: int
) -> None:
    """New addresses are due for a probe now (large-trade ones first); a large trade moves
    an existing address to the front of the queue unless it was probed in the last hour."""
    conn.executemany(
        """INSERT INTO addresses_seen (address, first_seen_ts, last_seen_ts, n_fills, taker_ntl,
                                       maker_ntl, max_trade_ntl, large, probe_after_ts)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT (address) DO UPDATE SET
             first_seen_ts = MIN(first_seen_ts, excluded.first_seen_ts),
             last_seen_ts = MAX(last_seen_ts, excluded.last_seen_ts),
             n_fills = n_fills + excluded.n_fills,
             taker_ntl = taker_ntl + excluded.taker_ntl,
             maker_ntl = maker_ntl + excluded.maker_ntl,
             max_trade_ntl = MAX(max_trade_ntl, excluded.max_trade_ntl),
             large = MAX(large, excluded.large),
             probe_after_ts = CASE
               WHEN excluded.large = 1
                    AND (last_probed_ts IS NULL OR last_probed_ts < ?) THEN 0
               ELSE probe_after_ts END""",
        [
            (addr, a.first_ts, a.last_ts, a.n, a.taker_ntl, a.maker_ntl, a.max_ntl,
             int(a.large), 0 if a.large else now, now - REPROBE_LARGE_AFTER_MS)
            for addr, a in activity.items()
        ],
    )  # fmt: skip


def due_probes(conn: sqlite3.Connection, now: int, limit: int) -> list[sqlite3.Row]:
    """Addresses due for a probe, large traders first, then by volume. Active watchlist
    wallets are polled every round, so they're skipped here."""
    return conn.execute(
        """SELECT a.* FROM addresses_seen a
           LEFT JOIN wallets w ON w.address = a.address AND w.status = 'active'
           WHERE a.probe_after_ts <= ? AND w.address IS NULL
           ORDER BY a.large DESC, (a.taker_ntl + a.maker_ntl) DESC
           LIMIT ?""",
        (now, limit),
    ).fetchall()


def record_probe(
    conn: sqlite3.Connection, address: str, now: int, ntl: float, next_probe: int
) -> None:
    conn.execute(
        """UPDATE addresses_seen SET last_probed_ts = ?, probe_ntl = ?, probe_after_ts = ?
           WHERE address = ?""",
        (now, ntl, next_probe, address),
    )


def get_activity(conn: sqlite3.Connection, address: str) -> sqlite3.Row | None:
    row: sqlite3.Row | None = conn.execute(
        "SELECT * FROM addresses_seen WHERE address = ?", (address,)
    ).fetchone()
    return row


# --- watchlist ------------------------------------------------------------------------


def get_wallet(conn: sqlite3.Connection, address: str) -> sqlite3.Row | None:
    row: sqlite3.Row | None = conn.execute(
        "SELECT * FROM wallets WHERE address = ?", (address,)
    ).fetchone()
    return row


def set_wallet(
    conn: sqlite3.Connection,
    address: str,
    status: str,
    reason: str,
    now: int,
    source: str | None = None,
    label: str | None = None,
    tags: str | None = None,
) -> None:
    """Insert or update a wallet's status, recording the change in wallet_events."""
    existing = get_wallet(conn, address)
    if existing is None:
        conn.execute(
            """INSERT INTO wallets (address, label, source, status, tags, added_ts, updated_ts)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (address, label, source or "trades", status, tags, now, now),
        )
    else:
        conn.execute(
            """UPDATE wallets SET status = ?, updated_ts = ?,
                 source = COALESCE(?, source), label = COALESCE(?, label),
                 tags = COALESCE(?, tags)
               WHERE address = ?""",
            (status, now, source, label, tags, address),
        )
        if existing["status"] == status:
            return
    conn.execute(
        """INSERT OR REPLACE INTO wallet_events (address, ts, status, reason)
           VALUES (?, ?, ?, ?)""",
        (address, now, status, reason),
    )


def add_tag(conn: sqlite3.Connection, address: str, tag: str) -> None:
    row = get_wallet(conn, address)
    if row is None:
        return
    tags = {x for x in (row["tags"] or "").split(",") if x}
    tags.add(tag)
    conn.execute("UPDATE wallets SET tags = ? WHERE address = ?", (",".join(sorted(tags)), address))


def active_wallets(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM wallets WHERE status = 'active' ORDER BY source, address"
    ).fetchall()


def active_at(events: Sequence[sqlite3.Row], ts: int) -> set[str]:
    """Addresses active at `ts`, from wallet_events rows sorted by ts."""
    status: dict[str, str] = {}
    for e in events:
        if e["ts"] > ts:
            break
        status[e["address"]] = e["status"]
    return {a for a, s in status.items() if s == "active"}


def load_events(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute("SELECT * FROM wallet_events ORDER BY ts, address").fetchall()


# --- positions ------------------------------------------------------------------------


def effective_leverage(pos: t.Position, state: t.ClearinghouseState) -> float | None:
    """Isolated: position value / margin posted for it. Cross: the account's total cross
    notional / its account value (all cross positions share that margin)."""
    if pos.leverage.type == "isolated":
        return pos.positionValue / pos.marginUsed if pos.marginUsed > 0 else None
    cross = state.crossMarginSummary
    return cross.totalNtlPos / cross.accountValue if cross.accountValue > 0 else None


@dataclass(frozen=True)
class PositionRow:
    round_ts: int
    address: str
    coin: str
    szi: float
    entry_px: float
    position_value: float
    lev_type: str
    lev: float
    eff_lev: float | None
    liq_px: float | None
    upnl: float
    margin_used: float
    account_value: float


def position_rows(
    round_ts: int, address: str, state: t.ClearinghouseState, coins: set[str]
) -> list[PositionRow]:
    rows = []
    for ap in state.assetPositions:
        p = ap.position
        if p.coin not in coins or p.szi == 0:
            continue
        rows.append(
            PositionRow(round_ts, address, p.coin, p.szi, p.entryPx, p.positionValue,
                        p.leverage.type, p.leverage.value, effective_leverage(p, state),
                        p.liquidationPx, p.unrealizedPnl, p.marginUsed,
                        state.marginSummary.accountValue)
        )  # fmt: skip
    return rows


def insert_positions(conn: sqlite3.Connection, rows: Sequence[PositionRow]) -> None:
    conn.executemany(
        """INSERT OR REPLACE INTO positions (round_ts, address, coin, szi, entry_px,
             position_value, lev_type, lev, eff_lev, liq_px, upnl, margin_used, account_value)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            (r.round_ts, r.address, r.coin, r.szi, r.entry_px, r.position_value, r.lev_type,
             r.lev, r.eff_lev, r.liq_px, r.upnl, r.margin_used, r.account_value)
            for r in rows
        ],
    )  # fmt: skip


def previous_round(conn: sqlite3.Connection, before: int) -> int | None:
    row = conn.execute(
        "SELECT MAX(round_ts) FROM snapshot_rounds WHERE round_ts < ?", (before,)
    ).fetchone()
    return None if row[0] is None else int(row[0])


def round_positions(conn: sqlite3.Connection, round_ts: int) -> dict[tuple[str, str], sqlite3.Row]:
    return {
        (r["address"], r["coin"]): r
        for r in conn.execute("SELECT * FROM positions WHERE round_ts = ?", (round_ts,))
    }


# --- liquidations ---------------------------------------------------------------------


def insert_liquidation(
    conn: sqlite3.Connection,
    source: str,
    tid: int | str,  # fill tid, or a provider's event id
    coin: str,
    ts: int,
    address: str,
    side: str,
    sz: float,
    px: float,
    method: str,
    quality: str = "partial",
) -> None:
    conn.execute(
        """INSERT OR IGNORE INTO liquidations (id, coin, ts, address, side, sz, px, ntl, method,
                                               source, quality)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (f"{source}:{tid}:{address}", coin, ts, address, side, sz, px, sz * px, method, source,
         quality),
    )  # fmt: skip


def liquidation_from_fill(
    conn: sqlite3.Connection, source: str, account: str, fill: t.UserFill, coins: set[str]
) -> bool:
    """Store a fill's liquidation, if it has one in a configured coin.

    For the liquidated account's own fills, a sell closes a long. The liquidator vault's
    backstop fills take over the position: it sells when taking over a short. Either way
    `liquidation.liquidatedUser` names the liquidated account when present."""
    liq = fill.liquidation
    if liq is None or fill.coin not in coins:
        return False
    liquidated = liq.liquidatedUser or account
    if liquidated == account:  # the liquidated account's own fill
        side = "long" if fill.side == "A" else "short"
    else:  # the liquidator's fill, taking over the position
        side = "short" if fill.side == "A" else "long"
    method = liq.method if liq.method in ("market", "backstop") else "unknown"
    insert_liquidation(
        conn, source, fill.tid, fill.coin, fill.time, liquidated, side, fill.sz, fill.px, method
    )
    return True
