"""The long-running recorder.

One asyncio process with these loops:
  * ws_loop (one per connection, `recorder.ws_connections`, default 2): a WebSocket with
    `trades` for every market and `activeAssetCtx` for every perp; reconnects with
    jittered backoff. Connections are redundant: trades are deduplicated by id, and a
    minute counts as recorded if any connection was live for all of it. Hyperliquid
    closes a connection after ~3h ("Expired"), so each is replaced after
    `rotate_after_s` while another one is healthy, and the expiry never costs data.
  * tick_loop: once a second, stages buffered trades, builds closed 1m bars, writes
    the last asset context per minute, confirms coverage, flushes finished hours to
    Parquet, checks for stale streams and writes health rows for `status`.
  * rest_loop: periodic REST polling (context + predicted funding, funding history,
    1h/1d candles) and 1m candle catch-up for gaps found on (re)connect.

All database writes happen synchronously on the event loop in short transactions, so
a shutdown between ticks never leaves half-written data.
"""

from __future__ import annotations

import asyncio
import contextlib
import functools
import json
import logging
import os
import random
import sqlite3
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import msgspec
import websockets
from websockets.asyncio.client import ClientConnection

from hlflows import coverage as cov
from hlflows import store
from hlflows.api import types as t
from hlflows.api.client import InfoClient
from hlflows.backfill import backfill_candles, backfill_funding, refresh_markets, snapshot_context
from hlflows.config import Config
from hlflows.data import STORED_RES, TAKER_STREAM, bars_stream
from hlflows.liquidations.providers import LiquidationProvider, make_provider
from hlflows.markets import Market
from hlflows.recorder.live import LiveWindow
from hlflows.recorder.trades import build_bar, flush_raw_trades, insert_trades
from hlflows.timeutil import DAY_MS, HOUR_MS, MINUTE_MS, floor_ms, fmt_ms, now_ms
from hlflows.wallets import store as wallet_store
from hlflows.wallets.tracker import WalletTracker

log = logging.getLogger(__name__)

CTX_STREAM = "ctx_1m"
Connect = Callable[[str], Awaitable[ClientConnection]]


def _default_connect(url: str) -> Awaitable[ClientConnection]:
    # Keepalive is done with Hyperliquid's application-level ping instead.
    return websockets.connect(url, ping_interval=None, max_size=2**24)


@dataclass
class WsConn:
    """One of the redundant WebSocket connections."""

    id: int
    connected: bool = False
    started: int = 0  # when the current session began
    last_msg_rx: int | None = None
    drops: int = 0  # sessions ended by the server or network
    rotations: int = 0  # sessions we replaced before the server's ~3h expiry
    rotating: bool = False
    reconnect: asyncio.Event = field(default_factory=asyncio.Event)


@dataclass
class MarketState:
    market: Market
    windows: dict[int, LiveWindow] = field(default_factory=dict)  # per connection
    built_upto: int = 0  # next minute to build
    last_trade_rx: int | None = None  # local receive time of the last trade (any connection)
    last_trade_ts: int | None = None  # exchange time of the last trade
    rx: dict[int, int] = field(default_factory=dict)  # last trade received, per connection
    inserted: int = 0

    def is_full(self, minute: int) -> bool:
        return any(w.is_full(minute) for w in self.windows.values())


@dataclass
class CtxState:
    coin: str
    windows: dict[int, LiveWindow] = field(default_factory=dict)  # per connection
    pending: tuple[int, t.PerpAssetCtx] | None = None  # (minute, latest sample in it)
    completed: list[tuple[int, t.PerpAssetCtx]] = field(default_factory=list)  # to write
    last_rx: int | None = None
    rx: dict[int, int] = field(default_factory=dict)  # last sample received, per connection
    samples: int = 0

    def is_full(self, minute: int) -> bool:
        return any(w.is_full(minute) for w in self.windows.values())


def _others_live(windows: dict[int, LiveWindow], cid: int) -> bool:
    return any(w.connected for k, w in windows.items() if k != cid)


class Recorder:
    def __init__(
        self,
        cfg: Config,
        conn: sqlite3.Connection,
        client: InfoClient,
        clock: Callable[[], int] = now_ms,
        connect: Connect = _default_connect,
    ) -> None:
        self.cfg = cfg
        self.rc = cfg.recorder
        self.conn = conn
        self.client = client
        self.clock = clock
        self.connect = connect
        self.trades_dir = Path(cfg.storage.data_dir) / "trades"

        self.markets: dict[str, MarketState] = {}
        self.by_hl: dict[str, str] = {}
        self.coin_of: dict[str, str] = {}  # market -> coin
        self.wallets = WalletTracker(cfg, conn, client, clock)
        self.provider: LiquidationProvider | None = make_provider(cfg.liquidation_provider)
        self.ctx: dict[str, CtxState] = {}  # by perp hl_name

        self.stop_event = asyncio.Event()
        self.conns = [WsConn(i) for i in range(self.rc.ws_connections)]
        self.stopped = False
        self.decode_errors = 0

        self._trade_buffer: list[t.WsTrade] = []
        self._coverage_buffer: list[tuple[str, str, int, int]] = []
        self._dirty: set[tuple[str, int]] = set()
        self._catchup: list[tuple[str, int, int]] = []  # (market, start, end) 1m candle gaps
        self._flushed_before = 0  # staged trades before this were moved to Parquet
        self._last_flush = 0
        self._last_heartbeat = 0
        self._rest_status: dict[str, tuple[int | None, str | None]] = {}

    @property
    def connected(self) -> bool:
        return any(c.connected for c in self.conns)

    @property
    def reconnects(self) -> int:
        return sum(c.drops for c in self.conns)

    @property
    def last_msg_rx(self) -> int | None:
        times = [c.last_msg_rx for c in self.conns if c.last_msg_rx is not None]
        return max(times) if times else None

    def _windows(self) -> dict[int, LiveWindow]:
        return {c.id: LiveWindow() for c in self.conns}

    # ------------------------------------------------------------------ setup

    async def setup(self) -> None:
        total, lines = self.cfg.weight_budget_lines()
        budget = "\n".join(lines)
        if total > self.cfg.weight_ceiling:
            raise RuntimeError(
                f"configured REST weight {total:.0f}/min exceeds the ceiling "
                f"{self.cfg.weight_ceiling}/min:\n{budget}\n"
                "Lower wallets.max_active, raise wallets.poll_interval_s, or raise "
                "rate_limit.ceiling_pct."
            )
        log.info("REST weight budget %.0f/min of %d:\n%s", total, self.cfg.weight_ceiling, budget)
        self.wallets.sync_manual()
        markets, warnings = await refresh_markets(self.cfg, self.conn, self.client)
        for w in warnings:
            log.warning("%s", w)
        wanted = set(self.cfg.markets.coins)
        start_minute = floor_ms(self.clock(), MINUTE_MS)
        for m in markets:
            if m.coin not in wanted:
                continue
            self.markets[m.market] = MarketState(m, self._windows(), built_upto=start_minute)
            self.by_hl[m.hl_name] = m.market
            self.coin_of[m.market] = m.coin
            if m.kind == "perp":
                self.ctx[m.hl_name] = CtxState(m.coin, self._windows())
        if not self.markets:
            raise RuntimeError("no markets resolved; check markets.coins in the config")
        log.info("recording %s", ", ".join(sorted(self.markets)))

    def subscriptions(self) -> list[dict[str, str]]:
        subs = [{"type": "trades", "coin": s.market.hl_name} for s in self.markets.values()]
        subs += [{"type": "activeAssetCtx", "coin": hl} for hl in self.ctx]
        return subs

    # ------------------------------------------------------------------ run

    async def run(self, duration_s: float | None = None) -> None:
        await self.setup()
        tasks = [
            *(asyncio.create_task(self.ws_loop(c), name=f"ws{c.id}") for c in self.conns),
            asyncio.create_task(self.tick_loop(), name="tick"),
            asyncio.create_task(self.rest_loop(), name="rest"),
            asyncio.create_task(self.wallet_loop(), name="wallets"),
        ]
        if self.provider is not None:
            tasks.append(asyncio.create_task(self.provider_loop(), name="provider"))
        stopper = None
        if duration_s is not None:
            stopper = asyncio.get_running_loop().call_later(duration_s, self.stop_event.set)
        try:
            stop_wait = asyncio.create_task(self.stop_event.wait())
            done, _ = await asyncio.wait([stop_wait, *tasks], return_when=asyncio.FIRST_COMPLETED)
            stop_wait.cancel()
            for task in done:
                if task in tasks and task.exception() is not None:
                    raise task.exception()  # type: ignore[misc]
        finally:
            if stopper is not None:
                stopper.cancel()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self.shutdown()

    def request_stop(self) -> None:
        self.stop_event.set()

    def shutdown(self) -> None:
        """Final flush: stage buffered trades, build every started minute (the current
        one as partial), write pending context, mark streams disconnected."""
        now = self.clock()
        for c in self.conns:
            self._on_disconnect(c)
        self.tick(now, final=True)
        self.stopped = True
        self.write_health(now)
        log.info("recorder stopped cleanly")

    # ------------------------------------------------------------------ websocket

    async def ws_loop(self, c: WsConn) -> None:
        # Stagger the connections' first subscribe bursts a little.
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self.stop_event.wait(), c.id * 3.0)
        attempt = 0
        while not self.stop_event.is_set():
            started = self.clock()
            try:
                ws = await self.connect(self.rc.ws_url)
                try:
                    await self._session(ws, c)
                finally:
                    await ws.close()
            except (OSError, TimeoutError, websockets.WebSocketException) as exc:
                if not c.rotating:
                    log.warning("websocket %d: %r", c.id, exc)
            finally:
                rotated = c.rotating
                self._on_disconnect(c)
            if self.stop_event.is_set():
                break
            if rotated:
                c.rotations += 1
                c.rotating = False
                attempt = 0
                delay = 0.1
            else:
                c.drops += 1
                if self.clock() - started > 5 * MINUTE_MS:
                    attempt = 0
                delay = min(self.rc.reconnect_cap_s, self.rc.reconnect_base_s * 2**attempt)
                delay = delay * (0.5 + random.random() / 2)
                attempt += 1
            log.info("connection %d reconnecting in %.1fs", c.id, delay)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self.stop_event.wait(), delay)

    async def _session(self, ws: ClientConnection, c: WsConn) -> None:
        c.reconnect.clear()
        c.connected = True
        c.started = c.last_msg_rx = self.clock()

        async def subscribe() -> None:
            # Paced, and concurrent with reading so nothing sent meanwhile is held back.
            for sub in self.subscriptions():
                await ws.send(json.dumps({"method": "subscribe", "subscription": sub}))
                await asyncio.sleep(self.rc.subscribe_pause_s)

        async def pinger() -> None:
            while True:
                await asyncio.sleep(self.rc.ping_interval_s)
                await ws.send('{"method":"ping"}')

        helpers = [asyncio.create_task(subscribe()), asyncio.create_task(pinger())]
        try:
            while not (self.stop_event.is_set() or c.reconnect.is_set()):
                recv = asyncio.create_task(ws.recv())
                stop = asyncio.create_task(self.stop_event.wait())
                again = asyncio.create_task(c.reconnect.wait())
                done, pending = await asyncio.wait(
                    [recv, stop, again],
                    timeout=self.rc.dead_after_s,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for p in pending:
                    p.cancel()
                # Collect them: a cancelled recv() can still finish with ConnectionClosed
                # when the socket closes, and an uncollected exception gets logged.
                await asyncio.gather(*pending, return_exceptions=True)
                if not done:
                    log.warning(
                        "connection %d: no messages for %.0fs; reconnecting",
                        c.id, self.rc.dead_after_s,
                    )  # fmt: skip
                    return
                if recv in done:
                    self.handle_message(recv.result(), self.clock(), c.id)
        finally:
            for task in helpers:
                task.cancel()
            # A send failing on a closed socket is reported by recv(); don't re-raise it.
            await asyncio.gather(*helpers, return_exceptions=True)

    def _on_disconnect(self, c: WsConn) -> None:
        if not c.connected:
            return
        # Nothing after the last message received is known to have been delivered.
        until = c.last_msg_rx if c.last_msg_rx is not None else self.clock()
        c.connected = False
        for s in self.markets.values():
            s.windows[c.id].on_disconnect(until)
        for x in self.ctx.values():
            x.windows[c.id].on_disconnect(until)

    # ------------------------------------------------------------------ messages

    def handle_message(self, raw: str | bytes, now: int, cid: int = 0) -> None:
        self.conns[cid].last_msg_rx = now
        try:
            self._handle(raw, now, cid)
        except (msgspec.DecodeError, msgspec.ValidationError) as exc:
            # Don't let one unexpected payload stop recording; surface it in logs and status.
            self.decode_errors += 1
            snippet = raw[:300] if isinstance(raw, str) else raw[:300].decode(errors="replace")
            log.error("could not decode message (%s): %s", exc, snippet)

    def _handle(self, raw: str | bytes, now: int, cid: int) -> None:
        env = t.decode(raw, t.WsEnvelope)
        if env.channel == "trades":
            trades = t.decode(env.data, list[t.WsTrade])
            for tr in trades:
                market = self.by_hl.get(tr.coin)
                if market is None:
                    continue
                state = self.markets[market]
                state.last_trade_rx = state.rx[cid] = now
                state.last_trade_ts = max(state.last_trade_ts or 0, tr.time)
                span = state.windows[cid].on_trade(tr.time)
                if span is not None:
                    for stream in (bars_stream("1m"), TAKER_STREAM):
                        self._coverage_buffer.append((stream, market, *span))
            self._trade_buffer.extend(trades)
        elif env.channel == "activeAssetCtx":
            msg = t.decode(env.data, t.WsActiveAssetCtx)
            c = self.ctx.get(msg.coin)
            if c is None:
                return
            minute = floor_ms(now, MINUTE_MS)
            if c.pending is not None and c.pending[0] != minute:
                c.completed.append(c.pending)  # written by the next tick
            c.pending = (minute, msg.ctx)
            c.last_rx = c.rx[cid] = now
            c.samples += 1
        elif env.channel == "subscriptionResponse":
            self._on_ack(msgspec.json.decode(env.data), now, cid)
        elif env.channel == "error":
            log.error("server error: %s", bytes(env.data)[:300].decode(errors="replace"))

    def _on_ack(self, data: Any, now: int, cid: int) -> None:
        """A subscription went live on connection `cid`. If no other connection was live
        for that stream, everything since its coverage ended is a gap."""
        sub = data.get("subscription", {}) if isinstance(data, dict) else {}
        kind, coin = sub.get("type"), sub.get("coin")
        if kind == "trades" and coin in self.by_hl:
            market = self.by_hl[coin]
            s = self.markets[market]
            covered = _others_live(s.windows, cid)
            live_from = s.windows[cid].on_ack(now)
            if covered:
                return
            self._record_resume_gap(market, TAKER_STREAM, live_from, "s3", "recorder not live")
            # OHLCV for the gap can still come from 1m candles (the taker split can't).
            gap_start = self._record_resume_gap(
                market, bars_stream("1m"), live_from, "rest", "recorder not live"
            )
            if gap_start is not None:
                self._catchup.append((market, gap_start, live_from))
        elif kind == "activeAssetCtx" and coin in self.ctx:
            c = self.ctx[coin]
            covered = _others_live(c.windows, cid)
            live_from = c.windows[cid].on_ack(now)
            if covered:
                return
            self._record_resume_gap(c.coin, CTX_STREAM, live_from, "s3", "recorder not live")

    def _last_covered(self, stream: str, market: str) -> int | None:
        row = self.conn.execute(
            "SELECT MAX(end_ts) FROM coverage WHERE stream = ? AND market = ?", (stream, market)
        ).fetchone()
        return None if row[0] is None else int(row[0])

    def _record_resume_gap(
        self, market: str, stream: str, live_from: int, recoverable: str, reason: str
    ) -> int | None:
        """Record [last coverage end, live_from) as a gap. Returns the gap start, if any."""
        last = self._last_covered(stream, market)
        if last is None or last >= live_from:
            return None
        with store.transaction(self.conn):
            cov.record_gap(self.conn, stream, market, last, live_from, reason, recoverable)
        log.warning("%s %s: gap %s → %s", stream, market, fmt_ms(last), fmt_ms(live_from))
        return last

    # ------------------------------------------------------------------ tick

    async def tick_loop(self) -> None:
        while not self.stop_event.is_set():
            await asyncio.sleep(1.0)
            self.tick(self.clock())

    def tick(self, now: int, final: bool = False) -> None:
        grace = int(self.rc.bar_grace_s * 1000)
        build_until = floor_ms(now - grace, MINUTE_MS)
        if final:  # build the minute in progress too (it will be marked partial)
            build_until = floor_ms(now, MINUTE_MS) + MINUTE_MS
        with store.transaction(self.conn):
            buffered, self._trade_buffer = self._trade_buffer, []
            inserted, touched = insert_trades(self.conn, self.by_hl, buffered)
            for market, _ in inserted:
                self.markets[market].inserted += 1
            activity = wallet_store.aggregate_activity(
                inserted, self.coin_of, self.cfg.wallets.large_trade_ntl
            )
            wallet_store.upsert_activity(self.conn, activity, now)
            for market, minute in touched:
                if minute < self.markets[market].built_upto:
                    if minute < self._flushed_before:
                        # Its hour is already in Parquet; staging holds only the late trades.
                        log.warning("late trade for %s %s not in bars", market, fmt_ms(minute))
                    else:
                        self._dirty.add((market, minute))

            bars: list[store.Bar] = []
            for s in self.markets.values():
                low = s.market.significance == "low"
                while s.built_upto < build_until:
                    m = s.built_upto
                    bar = build_bar(self.conn, s.market.market, m, not s.is_full(m), low)
                    if bar is not None:
                        bars.append(bar)
                    s.built_upto += MINUTE_MS
            for market, minute in sorted(self._dirty):
                s = self.markets[market]
                if minute >= s.built_upto:
                    continue  # will be built normally
                bar = build_bar(
                    self.conn, market, minute, not s.is_full(minute),
                    s.market.significance == "low",
                )  # fmt: skip
                if bar is not None:
                    bars.append(bar)
            self._dirty.clear()
            store.upsert_bars(self.conn, bars)

            coverage, self._coverage_buffer = self._coverage_buffer, []
            for stream, market, start, end in coverage:
                cov.add_coverage(self.conn, stream, market, start, end)

            minute_now = floor_ms(now, MINUTE_MS)
            for c in self.ctx.values():
                if c.pending is not None and (final or c.pending[0] < minute_now):
                    c.completed.append(c.pending)
                    c.pending = None
                for minute, ctx in c.completed:
                    self._write_ctx(c, minute, ctx)
                c.completed.clear()

        if final or now - self._last_flush >= MINUTE_MS:
            self._flush_parquet(now if final else now - int(self.rc.flush_grace_s * 1000))
            self._last_flush = now
        if not final:
            self._check_stale(now)
        if final or now - self._last_heartbeat >= self.rc.heartbeat_s * 1000:
            self.write_health(now)
            self._last_heartbeat = now

    def _write_ctx(self, c: CtxState, minute: int, ctx: t.PerpAssetCtx) -> None:
        store.upsert_asset_ctx(self.conn, c.coin, minute, ctx, "ws")
        if c.is_full(minute):
            cov.add_coverage(self.conn, CTX_STREAM, c.coin, minute, minute + MINUTE_MS)

    def _flush_parquet(self, before: int) -> None:
        # Only hours that finished before the flush horizon; the rest stay staged.
        horizon = floor_ms(before, HOUR_MS)
        with store.transaction(self.conn):
            written = flush_raw_trades(self.conn, self.trades_dir, horizon)
        self._flushed_before = max(self._flushed_before, horizon)
        for market, hour, n in written:
            log.info("wrote %s trades for %s %s", n, market, fmt_ms(hour))

    def _check_stale(self, now: int) -> None:
        """Per connection: reconnect it if one of its streams went stale; otherwise rotate
        it once it's older than rotate_after_s, if another connection is healthy (so a
        stream is live throughout) and no other rotation is in progress."""
        healthy = {
            c.id for c in self.conns
            if c.connected and not c.rotating and not c.reconnect.is_set()
            and now - c.started >= MINUTE_MS
        }  # fmt: skip
        for c in self.conns:
            if not c.connected or c.reconnect.is_set():
                continue
            reasons = []
            for s in self.markets.values():
                w = s.windows[c.id]
                if s.market.kind != "perp" or w.live_from is None:
                    continue
                last = max(s.rx.get(c.id, 0), w.live_from)
                if now - last > self.rc.stale_trades_s * 1000:
                    reasons.append(f"no {s.market.market} trades for {(now - last) / 1000:.0f}s")
            for x in self.ctx.values():
                w = x.windows[c.id]
                if w.live_from is None:
                    continue
                last = max(x.rx.get(c.id, 0), w.live_from)
                if now - last > self.rc.stale_ctx_s * 1000:
                    reasons.append(f"no {x.coin} asset context for {(now - last) / 1000:.0f}s")
            if reasons:
                log.warning("connection %d stale (%s); reconnecting", c.id, "; ".join(reasons))
                c.reconnect.set()
                healthy.discard(c.id)
                continue
            age = now - c.started
            if (
                self.rc.rotate_after_s > 0
                and age >= self.rc.rotate_after_s * 1000
                and healthy - {c.id}
                and not any(o.rotating for o in self.conns)
            ):
                log.info("rotating connection %d after %.1fh (the server expires connections "
                         "after ~3h)", c.id, age / HOUR_MS)  # fmt: skip
                c.rotating = True
                c.reconnect.set()
                healthy.discard(c.id)

    # ------------------------------------------------------------------ health

    def _recorder_state(self) -> str:
        if self.stopped:
            return "stopped"
        return "connected" if self.connected else "disconnected"

    def write_health(self, now: int) -> None:
        rows: list[tuple[str, int | None, int | None, int, str | None]] = [
            ("recorder", now, now, os.getpid(), self._recorder_state()),
            (
                "ws",
                self.last_msg_rx,
                now if self.connected else None,
                self.reconnects,
                ", ".join(
                    [
                        f"{sum(c.connected for c in self.conns)}/{len(self.conns)} connected",
                        f"{sum(c.rotations for c in self.conns)} rotations",
                    ]
                    + ([f"{self.decode_errors} undecodable messages"] if self.decode_errors else [])
                ),
            ),
        ]
        for s in self.markets.values():
            rows.append((f"trades:{s.market.market}", s.last_trade_ts, s.last_trade_rx,
                         s.inserted, None))  # fmt: skip
        for c in self.ctx.values():
            rows.append((f"ctx:{c.coin}", c.last_rx, c.last_rx, c.samples, None))
        for name, (ok_ts, err) in self._rest_status.items():
            rows.append((f"rest:{name}", ok_ts, ok_ts, 0, err))
        with store.transaction(self.conn):
            self.conn.executemany(
                """INSERT INTO stream_state (stream, last_msg_ts, last_ok_ts, count, detail,
                                             updated_ts)
                   VALUES (?, ?, ?, ?, ?, ?)
                   ON CONFLICT (stream) DO UPDATE SET last_msg_ts = excluded.last_msg_ts,
                     last_ok_ts = COALESCE(excluded.last_ok_ts, stream_state.last_ok_ts),
                     count = excluded.count, detail = excluded.detail,
                     updated_ts = excluded.updated_ts""",
                [(*r, now) for r in rows],
            )

    # ------------------------------------------------------------------ REST

    async def rest_loop(self) -> None:
        next_context = 0
        next_candles = 0
        next_funding = 0
        while not self.stop_event.is_set():
            now = self.clock()
            if now >= next_context:
                await self._rest("context", self._poll_context)
                next_context = now + int(self.rc.context_poll_s * 1000)
            if now >= next_funding:
                await self._rest("funding", self._poll_funding)
                offset = int(self.rc.funding_poll_offset_s * 1000)
                next_funding = floor_ms(now - offset, HOUR_MS) + HOUR_MS + offset
            if now >= next_candles:
                await self._rest("candles", self._poll_candles)
                next_candles = now + int(self.rc.candle_catchup_s * 1000)
            # Catch up gaps once their last minute's candle has closed.
            due = [g for g in self._catchup if g[2] + 10_000 <= now]
            self._catchup = [g for g in self._catchup if g not in due]
            for market, start, end in due:
                await self._rest(
                    "gap-candles", functools.partial(self._catch_up, market, start, end)
                )
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self.stop_event.wait(), 5.0)

    async def wallet_loop(self) -> None:
        """Watchlist polling rounds, liquidator fills hourly, probing in between."""
        interval = int(self.cfg.wallets.poll_interval_s * 1000)
        next_round = 0
        next_liquidator = 0
        while not self.stop_event.is_set():
            now = self.clock()
            if now >= next_round:
                next_round = now + interval
                await self._rest("wallet-round", self.wallets.poll_round)
            if now >= next_liquidator:
                next_liquidator = now + HOUR_MS
                await self._rest("liquidator-fills", self.wallets.poll_liquidators)
            await self._rest("roles", self.wallets.check_roles)
            await self._rest("probe", self.wallets.probe_due)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self.stop_event.wait(), 10.0)

    async def provider_loop(self) -> None:
        """Poll the optional paid liquidation provider (off unless configured)."""
        assert self.provider is not None
        interval = self.cfg.liquidation_provider.poll_s
        coins = set(self.cfg.markets.coins)
        stream = f"provider:{self.provider.name}"
        try:
            while not self.stop_event.is_set():
                await self._rest(stream, lambda: self.poll_provider(coins, stream))
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self.stop_event.wait(), interval)
        finally:
            await self.provider.aclose()

    async def poll_provider(self, coins: set[str], stream: str) -> int:
        assert self.provider is not None
        now = self.clock()
        start = self._last_covered(stream, "all")
        start = now - HOUR_MS if start is None else start
        events = await self.provider.fetch(coins, start, now)
        with store.transaction(self.conn):
            for e in events:
                wallet_store.insert_liquidation(
                    self.conn, "provider", e.id, e.coin, e.ts, e.address, e.side, e.sz, e.px,
                    e.method if e.method in ("market", "backstop") else "unknown",
                    quality="external",
                )  # fmt: skip
            cov.add_coverage(self.conn, stream, "all", start, now)
        return len(events)

    async def _rest(self, name: str, fn: Callable[[], Awaitable[object]]) -> None:
        try:
            await fn()
            self._rest_status[name] = (self.clock(), None)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # keep recording; report in `status`
            log.exception("REST %s failed", name)
            prev = self._rest_status.get(name, (None, None))[0]
            self._rest_status[name] = (prev, repr(exc)[:200])

    async def _poll_context(self) -> None:
        await snapshot_context(self.cfg, self.conn, self.client, list(self.cfg.markets.coins))

    async def _poll_funding(self) -> None:
        now = self.clock()
        for s in self.markets.values():
            if s.market.kind != "perp":
                continue
            row = self.conn.execute(
                "SELECT MAX(ts) FROM funding WHERE coin = ?", (s.market.coin,)
            ).fetchone()
            start = (row[0] - 2 * HOUR_MS) if row[0] is not None else now - 7 * DAY_MS
            await backfill_funding(self.conn, self.client, s.market.coin, start, now)

    async def _poll_candles(self) -> None:
        now = self.clock()
        for s in self.markets.values():
            for res in ("1h", "1d"):
                last = self._last_covered(bars_stream(res), s.market.market)
                start = (last - STORED_RES[res]) if last is not None else now - 30 * DAY_MS
                await backfill_candles(self.conn, self.client, s.market, res, start, now)

    async def _catch_up(self, market: str, start: int, end: int) -> None:
        log.info("1m candle catch-up %s %s → %s", market, fmt_ms(start), fmt_ms(end))
        await backfill_candles(
            self.conn, self.client, self.markets[market].market, "1m", start, end
        )
