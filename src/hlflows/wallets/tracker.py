"""The wallet watchlist: discovery by probing trade addresses, polling rounds, dormancy,
market-maker and vault tagging, and liquidation checks.

Budget: polling costs 2 weight per active wallet per round; probing and userRole checks
share their own per-minute sub-budget (`wallets.probe_weight_per_min`); liquidation fill
checks are capped per round. Everything also goes through the shared REST budget.
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass

from hlflows import coverage as cov
from hlflows import store
from hlflows.api import types as t
from hlflows.api.client import ApiError, InfoClient
from hlflows.config import Config
from hlflows.timeutil import DAY_MS, HOUR_MS, MINUTE_MS, fmt_ms, now_ms
from hlflows.wallets import store as ws

log = logging.getLogger(__name__)

POSITIONS_STREAM = "positions"
PROBE_WEIGHT = 2
ROLE_WEIGHT = 60


@dataclass(frozen=True)
class ProbeResult:
    address: str
    ntl: float  # notional in configured coins
    qualifies: bool
    outcome: str  # activated | displaced-smaller | full | not-qualified | mm | vault | error


class WalletTracker:
    def __init__(
        self,
        cfg: Config,
        conn: sqlite3.Connection,
        client: InfoClient,
        clock: Callable[[], int] = now_ms,
    ) -> None:
        self.cfg = cfg
        self.w = cfg.wallets
        self.conn = conn
        self.client = client
        self.clock = clock
        self.coins = set(cfg.markets.coins)
        self.vaults = {a.lower() for a in self.w.known_vaults}
        self.liquidators = {a.lower() for a in self.w.liquidators}
        self._spent: deque[tuple[int, int]] = deque()  # (ts, weight) of prober spend
        self._fill_checks: deque[tuple[str, int, int]] = deque()  # (address, start, end)
        self._last_role_check = 0

    # ------------------------------------------------------------------ manual list

    def sync_manual(self) -> None:
        """Make every configured manual wallet active; manual wallets never auto-change."""
        now = self.clock()
        with store.transaction(self.conn):
            for address in self.w.manual:
                label = self.w.labels.get(address)
                row = ws.get_wallet(self.conn, address)
                if row is None or row["status"] != "active" or row["source"] != "manual":
                    ws.set_wallet(self.conn, address, "active", "manual (config)", now,
                                  source="manual", label=label)  # fmt: skip
                elif label and row["label"] != label:
                    self.conn.execute(
                        "UPDATE wallets SET label = ? WHERE address = ?", (label, address)
                    )

    # ------------------------------------------------------------------ sub-budget

    def _available(self, now: int) -> int:
        while self._spent and self._spent[0][0] <= now - MINUTE_MS:
            self._spent.popleft()
        return self.w.probe_weight_per_min - sum(w for _, w in self._spent)

    def _spend(self, now: int, weight: int) -> None:
        self._spent.append((now, weight))

    # ------------------------------------------------------------------ probing

    def qualifying_ntl(self, state: t.ClearinghouseState) -> tuple[float, bool]:
        total, qualifies = 0.0, False
        for ap in state.assetPositions:
            p = ap.position
            if p.coin in self.coins and p.szi != 0:
                total += p.positionValue
                qualifies = qualifies or p.positionValue >= self.w.qualify_ntl[p.coin]
        return total, qualifies

    def is_market_maker(self, activity: sqlite3.Row | None, ntl: float) -> bool:
        if activity is None or activity["n_fills"] < self.w.mm_min_fills:
            return False
        volume = activity["taker_ntl"] + activity["maker_ntl"]
        if volume <= 0:
            return False
        maker_share = float(activity["maker_ntl"]) / volume
        return bool(
            maker_share >= self.w.mm_min_maker_share
            and ntl / volume <= self.w.mm_max_position_to_volume
        )

    async def probe_due(self, limit: int | None = None, dry_run: bool = False) -> list[ProbeResult]:
        """Probe due addresses within the prober's sub-budget (or up to `limit`)."""
        now = self.clock()
        n = limit if limit is not None else self._available(now) // PROBE_WEIGHT
        if n <= 0:
            return []
        results = []
        for row in ws.due_probes(self.conn, now, n):
            if limit is None:
                self._spend(self.clock(), PROBE_WEIGHT)
            results.append(await self.probe(row["address"], dry_run))
        return results

    async def probe(self, address: str, dry_run: bool = False) -> ProbeResult:
        now = self.clock()
        try:
            state = await self.client.clearinghouse_state(address, self.cfg.markets.dex)
        except ApiError as exc:
            log.warning("probe %s failed: %s", address, exc)
            return ProbeResult(address, 0.0, False, "error")
        ntl, qualifies = self.qualifying_ntl(state)
        activity = ws.get_activity(self.conn, address)
        reprobe = now + int(self.w.reprobe_days * DAY_MS)

        if not qualifies:
            outcome = "not-qualified"
        elif address in self.vaults:
            outcome = "vault"
        elif self.is_market_maker(activity, ntl):
            outcome = "mm"
        else:
            outcome = self._placement(address, ntl)
        if dry_run:
            return ProbeResult(address, ntl, qualifies, outcome)

        with store.transaction(self.conn):
            ws.record_probe(self.conn, address, now, ntl, reprobe)
            if outcome in ("vault", "mm") and self._excluded_tag(outcome):
                ws.set_wallet(self.conn, address, "dormant", f"tagged {outcome}", now, tags=outcome)
            elif outcome in ("activated", "displaced-smaller"):
                if outcome == "displaced-smaller":
                    self._displace_smallest(now)
                ws.set_wallet(self.conn, address, "active", f"probe: ${ntl:,.0f}", now,
                              source="trades")  # fmt: skip
                self.conn.execute(
                    """UPDATE wallets SET qualifying_ntl = ?, last_qualified_ts = ?,
                         account_value = ? WHERE address = ?""",
                    (ntl, now, state.marginSummary.accountValue, address),
                )
        if outcome in ("activated", "displaced-smaller"):
            log.info(
                "watching %s ($%s in %s)", address, f"{ntl:,.0f}", ",".join(sorted(self.coins))
            )
        return ProbeResult(address, ntl, qualifies, outcome)

    def _excluded_tag(self, tag: str) -> bool:
        return (tag == "vault" and self.w.exclude_vaults) or (tag == "mm" and self.w.exclude_mm)

    def _placement(self, address: str, ntl: float) -> str:
        discovered = self.conn.execute(
            "SELECT COUNT(*) FROM wallets WHERE status = 'active' AND source = 'trades'"
        ).fetchone()[0]
        manual = self.conn.execute(
            "SELECT COUNT(*) FROM wallets WHERE status = 'active' AND source = 'manual'"
        ).fetchone()[0]
        if discovered + manual < self.w.max_active:
            return "activated"
        smallest = self._smallest_discovered()
        if smallest is not None and ntl > (smallest["qualifying_ntl"] or 0.0):
            return "displaced-smaller"
        return "full"

    def _smallest_discovered(self) -> sqlite3.Row | None:
        row: sqlite3.Row | None = self.conn.execute(
            """SELECT * FROM wallets WHERE status = 'active' AND source = 'trades'
               ORDER BY COALESCE(qualifying_ntl, 0) ASC LIMIT 1"""
        ).fetchone()
        return row

    def _displace_smallest(self, now: int) -> None:
        smallest = self._smallest_discovered()
        if smallest is None:
            return
        ws.set_wallet(self.conn, smallest["address"], "dormant", "displaced by larger", now)
        ws.record_probe(self.conn, smallest["address"], now, smallest["qualifying_ntl"] or 0.0,
                        now + int(self.w.reprobe_days * DAY_MS))  # fmt: skip

    async def check_roles(self) -> int:
        """userRole (weight 60) for one active wallet not yet checked, at most once a minute
        and within the sub-budget, so probing keeps the rest. Vaults found this way are
        tagged and, if excluded, made dormant."""
        checked = 0
        now = self.clock()
        if now - self._last_role_check < MINUTE_MS:
            return 0
        while checked == 0 and self._available(now) >= ROLE_WEIGHT:
            row = self.conn.execute(
                "SELECT address, source FROM wallets WHERE status = 'active' AND role IS NULL"
                " ORDER BY qualifying_ntl DESC LIMIT 1"
            ).fetchone()
            if row is None:
                break
            self._spend(self.clock(), ROLE_WEIGHT)
            self._last_role_check = now
            try:
                role = (await self.client.user_role(row["address"])).role
            except ApiError as exc:
                log.warning("userRole %s failed: %s", row["address"], exc)
                break
            now = self.clock()
            with store.transaction(self.conn):
                self.conn.execute(
                    "UPDATE wallets SET role = ? WHERE address = ?", (role, row["address"])
                )
                if role == "vault":
                    ws.add_tag(self.conn, row["address"], "vault")
                    if self.w.exclude_vaults and row["source"] != "manual":
                        ws.set_wallet(self.conn, row["address"], "dormant", "userRole vault", now)
            checked += 1
        return checked

    # ------------------------------------------------------------------ polling rounds

    async def poll_round(self) -> int:
        """Poll every active wallet once and store one snapshot round. Returns round_ts."""
        round_ts = self.clock()
        started = round_ts
        wallets = ws.active_wallets(self.conn)
        sem = asyncio.Semaphore(self.w.poll_concurrency)

        async def fetch(address: str) -> tuple[str, t.ClearinghouseState | None]:
            async with sem:
                try:
                    return address, await self.client.clearinghouse_state(
                        address, self.cfg.markets.dex
                    )
                except ApiError as exc:
                    log.warning("poll %s failed: %s", address, exc)
                    return address, None

        results = await asyncio.gather(*(fetch(w["address"]) for w in wallets))
        now = self.clock()
        prev_ts = ws.previous_round(self.conn, round_ts)
        prev = ws.round_positions(self.conn, prev_ts) if prev_ts is not None else {}
        by_address = {w["address"]: w for w in wallets}
        rows: list[ws.PositionRow] = []
        failed = []
        with store.transaction(self.conn):
            for address, state in results:
                if state is None:
                    failed.append(address)
                    continue
                rows.extend(ws.position_rows(round_ts, address, state, self.coins))
                ntl, qualifies = self.qualifying_ntl(state)
                self.conn.execute(
                    """UPDATE wallets SET qualifying_ntl = ?, account_value = ?,
                         last_qualified_ts = CASE WHEN ? THEN ? ELSE last_qualified_ts END
                       WHERE address = ?""",
                    (ntl, state.marginSummary.accountValue, qualifies, round_ts, address),
                )
                if not self._maybe_market_maker(by_address[address], ntl, now):
                    self._maybe_dormant(by_address[address], qualifies, now)
            ws.insert_positions(self.conn, rows)
            self.conn.executemany(
                "INSERT OR IGNORE INTO round_failures (round_ts, address) VALUES (?, ?)",
                [(round_ts, a) for a in failed],
            )
            self.conn.execute(
                """INSERT OR REPLACE INTO snapshot_rounds
                   (round_ts, wallets_polled, wallets_failed, duration_ms) VALUES (?, ?, ?, ?)""",
                (round_ts, len(wallets), len(failed), now - started),
            )
            interval = int(self.w.poll_interval_s * 1000)
            last = self.conn.execute(
                "SELECT MAX(end_ts) FROM coverage WHERE stream = ? AND market = 'watchlist'",
                (POSITIONS_STREAM,),
            ).fetchone()[0]
            if last is not None and round_ts - last > interval:
                cov.record_gap(self.conn, POSITIONS_STREAM, "watchlist", last, round_ts,
                               "no polling rounds", "none")  # fmt: skip
            cov.add_coverage(
                self.conn, POSITIONS_STREAM, "watchlist", round_ts, round_ts + interval
            )

        current = {(r.address, r.coin): r for r in rows}
        self._queue_fill_checks(prev, current, failed, prev_ts, round_ts)
        await self._run_fill_checks()
        return round_ts

    def _maybe_market_maker(self, wallet: sqlite3.Row, ntl: float, now: int) -> bool:
        """Re-apply the market-maker heuristic every round: at first probe the recorder may
        not have seen enough of an address's fills to tell."""
        if wallet["source"] == "manual" or not self.w.exclude_mm:
            return False
        if not self.is_market_maker(ws.get_activity(self.conn, wallet["address"]), ntl):
            return False
        ws.set_wallet(self.conn, wallet["address"], "dormant", "tagged mm", now)
        ws.add_tag(self.conn, wallet["address"], "mm")
        return True

    def _maybe_dormant(self, wallet: sqlite3.Row, qualifies: bool, now: int) -> None:
        if wallet["source"] == "manual" or qualifies:
            return
        last = wallet["last_qualified_ts"] or wallet["added_ts"]
        if now - last >= self.w.dormant_after_h * HOUR_MS:
            ws.set_wallet(self.conn, wallet["address"], "dormant", "no qualifying position", now)
            ws.record_probe(self.conn, wallet["address"], now, wallet["qualifying_ntl"] or 0.0,
                            now + int(self.w.reprobe_days * DAY_MS))  # fmt: skip

    def _mark(self, coin: str) -> float | None:
        row = self.conn.execute(
            "SELECT mark_px FROM asset_ctx WHERE coin = ? ORDER BY ts DESC LIMIT 1", (coin,)
        ).fetchone()
        return None if row is None else float(row[0])

    def _queue_fill_checks(
        self,
        prev: dict[tuple[str, str], sqlite3.Row],
        current: dict[tuple[str, str], ws.PositionRow],
        failed: list[str],
        prev_ts: int | None,
        round_ts: int,
    ) -> None:
        """A position that shrank or vanished while its liquidation price was near the mark
        may have been liquidated: queue a userFillsByTime check for that window."""
        if prev_ts is None:
            return
        queued: set[str] = set()
        for (address, coin), p in prev.items():
            if address in failed or address in queued or p["liq_px"] is None:
                continue
            cur = current.get((address, coin))
            cur_size = abs(cur.szi) if cur is not None else 0.0
            flipped = cur is not None and (cur.szi > 0) != (p["szi"] > 0)
            if cur_size >= abs(p["szi"]) and not flipped:
                continue
            mark = self._mark(coin)
            if mark is None:
                continue
            if abs(mark - p["liq_px"]) / mark * 100 <= self.w.liq_check_distance_pct:
                self._fill_checks.append((address, prev_ts, round_ts))
                queued.add(address)

    async def _run_fill_checks(self) -> int:
        n = 0
        checks = 0
        while self._fill_checks and checks < self.w.max_fill_checks_per_round:
            address, start, end = self._fill_checks.popleft()
            n += await self.fetch_liquidations(address, start, end, "watched_fills") or 0
            checks += 1
        return n

    async def fetch_liquidations(
        self, address: str, start: int, end: int, source: str
    ) -> int | None:
        """Store liquidation fills for `address` in [start, end]. None if the request failed."""
        try:
            fills = await self.client.user_fills_by_time(address, start, end)
        except ApiError as exc:
            log.warning("userFillsByTime %s failed: %s", address, exc)
            return None
        n = 0
        with store.transaction(self.conn):
            for f in fills:
                if ws.liquidation_from_fill(self.conn, source, address, f, self.coins):
                    n += 1
        if n:
            log.info("%s liquidation fill(s) for %s since %s", n, address, fmt_ms(start))
        return n

    async def poll_liquidators(self) -> int:
        """Backstop liquidations, exchange-wide: the liquidator vault's own fills."""
        now = self.clock()
        n = 0
        for address in self.liquidators:
            row = self.conn.execute(
                "SELECT MAX(end_ts) FROM coverage WHERE stream = 'liquidator_fills' AND market = ?",
                (address,),
            ).fetchone()
            start = row[0] if row[0] is not None else now - 7 * DAY_MS
            found = await self.fetch_liquidations(address, start, now, "liquidator_fills")
            if found is None:
                continue
            n += found
            with store.transaction(self.conn):
                cov.add_coverage(self.conn, "liquidator_fills", address, start, now)
        return n
