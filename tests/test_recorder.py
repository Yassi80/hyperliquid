from __future__ import annotations

import asyncio
import json
import sqlite3
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pytest
import websockets
from websockets.asyncio.server import ServerConnection, serve

from hlflows import coverage as cov
from hlflows import data
from hlflows.api.client import InfoClient
from hlflows.budget import WeightBudget
from hlflows.config import Config, RecorderConfig, StorageConfig
from hlflows.recorder.core import CTX_STREAM, Recorder
from hlflows.timeutil import HOUR_MS, MINUTE_MS, TIMEFRAMES, to_ms
from tests.conftest import fixture_json
from tests.fake_api import URL, FakeInfo

M0 = to_ms(datetime(2026, 8, 3, 10, 0, tzinfo=UTC))


class Clock:
    def __init__(self, t: int) -> None:
        self.t = t

    def __call__(self) -> int:
        return self.t


def trades_msg(*trades: tuple[int, int, str, float, float], coin: str = "BTC") -> str:
    return json.dumps({
        "channel": "trades",
        "data": [
            {"coin": coin, "side": side, "px": str(px), "sz": str(sz), "time": ts,
             "hash": f"0x{tid:x}", "tid": tid, "users": ["0xb", "0xs"]}
            for tid, ts, side, px, sz in trades
        ],
    })  # fmt: skip


def ctx_msg(coin: str, oi: float) -> str:
    ctx = fixture_json("metaAndAssetCtxs.json")[1][0]
    return json.dumps({"channel": "activeAssetCtx", "data": {"coin": coin, "ctx": {**ctx,
                       "openInterest": str(oi)}}})  # fmt: skip


def ack(kind: str, coin: str) -> str:
    return json.dumps({"channel": "subscriptionResponse", "data": {"method": "subscribe",
                       "subscription": {"type": kind, "coin": coin}}})  # fmt: skip


@pytest.fixture
def cfg(tmp_path: Path) -> Config:
    return Config(
        storage=StorageConfig(db_path=str(tmp_path / "db.sqlite"), data_dir=str(tmp_path)),
        recorder=RecorderConfig(
            subscribe_pause_s=0, reconnect_base_s=0.01, reconnect_cap_s=0.05,
            ping_interval_s=0.05, dead_after_s=0.3, ws_connections=1,
        ),
    )  # fmt: skip


@pytest.fixture
async def recorder(cfg: Config, conn: sqlite3.Connection) -> AsyncIterator[Recorder]:
    http = httpx.AsyncClient(transport=httpx.MockTransport(FakeInfo()))
    async with InfoClient(WeightBudget(conn, 10_000), URL, http=http) as client:
        rec = Recorder(cfg, conn, client, clock=Clock(M0 + 20_000))
        await rec.setup()
        yield rec


def ack_all(rec: Recorder, now: int, cid: int = 0) -> None:
    rec.conns[cid].connected = True
    rec.conns[cid].started = now
    for sub in rec.subscriptions():
        rec.handle_message(ack(sub["type"], sub["coin"]), now, cid)


def bar_row(conn: sqlite3.Connection, market: str, ts: int) -> sqlite3.Row:
    row: sqlite3.Row = conn.execute(
        "SELECT * FROM bars WHERE market = ? AND res = '1m' AND ts = ?", (market, ts)
    ).fetchone()
    return row


async def test_bars_coverage_and_dedupe(recorder: Recorder, conn: sqlite3.Connection) -> None:
    rec = recorder
    M1, M2, M3 = M0 + MINUTE_MS, M0 + 2 * MINUTE_MS, M0 + 3 * MINUTE_MS
    ack_all(rec, M0 + 20_000)  # live from M1
    rec.handle_message(trades_msg((1, M0 + 5_000, "B", 100, 1)), M0 + 20_100)  # replay batch
    rec.handle_message(
        trades_msg((2, M1 + 1_000, "B", 101, 2), (3, M1 + 30_000, "A", 99, 1)), M1 + 30_100
    )
    rec.handle_message(trades_msg((4, M2 + 2_000, "B", 102, 1)), M2 + 2_100)
    rec.handle_message(trades_msg((2, M1 + 1_000, "B", 101, 2)), M2 + 2_200)  # replayed again
    rec.tick(M2 + 6_000)

    m0 = bar_row(conn, "BTC", M0)
    assert m0["quality"] == "partial" and m0["n"] == 1  # recorder wasn't live all minute
    m1 = bar_row(conn, "BTC", M1)
    assert m1["quality"] is None and m1["n"] == 2
    assert m1["buy_sz"] == 2 and m1["sell_sz"] == 1
    # The M2 trade proves the stream delivered all of M1.
    assert cov.get_coverage(conn, data.TAKER_STREAM, "BTC") == [(M1, M2)]
    assert cov.get_coverage(conn, data.bars_stream("1m"), "BTC") == [(M1, M2)]

    # A late trade for an already-built minute rebuilds it.
    rec.handle_message(trades_msg((5, M1 + 59_000, "A", 98, 0.5)), M2 + 7_000)
    rec.tick(M2 + 8_000)
    m1 = bar_row(conn, "BTC", M1)
    assert m1["n"] == 3 and m1["c"] == 98 and m1["sell_sz"] == 1.5

    rec.tick(M3 + 6_000)
    df = data.load_bars(conn, "BTC", TIMEFRAMES["1m"], M1, M3)
    first = df.row(0, named=True)
    assert first["taker"] and first["delta"] == pytest.approx(0.5)
    assert df.row(1, named=True)["taker"] is False  # M2 not confirmed yet


async def test_asset_context_one_sample_per_minute(
    recorder: Recorder, conn: sqlite3.Connection
) -> None:
    rec = recorder
    M1, M2 = M0 + MINUTE_MS, M0 + 2 * MINUTE_MS
    ack_all(rec, M0 + 20_000)
    rec.handle_message(ctx_msg("BTC", 100.0), M1 + 1_000)
    rec.handle_message(ctx_msg("BTC", 101.0), M1 + 50_000)  # last sample in M1 wins
    rec.handle_message(ctx_msg("BTC", 102.0), M2 + 1_000)
    rec.tick(M2 + 2_000)
    rows = conn.execute("SELECT ts, oi, source FROM asset_ctx WHERE coin = 'BTC'").fetchall()
    ws_rows = [tuple(r) for r in rows if r["source"] == "ws"]
    assert ws_rows == [(M1, 101.0, "ws")]
    assert cov.get_coverage(conn, CTX_STREAM, "BTC") == [(M1, M2)]


async def test_resume_records_gaps_and_queues_catchup(
    recorder: Recorder, conn: sqlite3.Connection
) -> None:
    rec = recorder
    M1, M2 = M0 + MINUTE_MS, M0 + 2 * MINUTE_MS
    ack_all(rec, M0 + 20_000)
    rec.handle_message(trades_msg((1, M1 + 1_000, "B", 100, 1)), M1 + 1_100)
    rec.handle_message(trades_msg((2, M2 + 1_000, "B", 100, 1)), M2 + 1_100)
    rec.tick(M2 + 6_000)
    rec._on_disconnect(rec.conns[0])

    resume = M0 + 7 * MINUTE_MS + 10_000
    ack_all(rec, resume)
    gaps = {
        r["stream"]: (r["start_ts"], r["end_ts"], r["recoverable"])
        for r in conn.execute("SELECT * FROM gaps WHERE market = 'BTC'")
    }
    assert gaps["taker_1m"] == (M2, M0 + 8 * MINUTE_MS, "s3")
    assert gaps["bars_1m"] == (M2, M0 + 8 * MINUTE_MS, "rest")
    assert ("BTC", M2, M0 + 8 * MINUTE_MS) in rec._catchup


async def test_shutdown_flushes_everything(recorder: Recorder, conn: sqlite3.Connection) -> None:
    rec = recorder
    ack_all(rec, M0 + 20_000)
    rec.handle_message(trades_msg((1, M0 + HOUR_MS - 1_000, "B", 100, 1)), M0 + HOUR_MS)
    rec.handle_message(trades_msg((2, M0 + HOUR_MS + 5_000, "A", 100, 1)), M0 + HOUR_MS + 5_000)
    assert isinstance(rec.clock, Clock)
    rec.clock.t = M0 + HOUR_MS + 10_000
    rec.shutdown()
    assert bar_row(conn, "BTC", M0 + HOUR_MS)["quality"] == "partial"  # current minute
    assert conn.execute("SELECT COUNT(*) FROM trades_staging").fetchone()[0] == 1  # open hour
    assert (Path(rec.cfg.storage.data_dir) / "trades" / "market=BTC").exists()
    state = conn.execute("SELECT detail FROM stream_state WHERE stream = 'recorder'").fetchone()
    assert state[0] == "stopped"


# --- a real (local) WebSocket server --------------------------------------------------


class FakeWsServer:
    """Acks subscriptions and sends one trade per connection. The first connection is
    closed by the server after that; later ones stay open (optionally silent)."""

    def __init__(self, silent_after_first: bool = False) -> None:
        self.connections = 0
        self.received: list[dict[str, Any]] = []
        self.silent_after_first = silent_after_first

    async def handler(self, ws: ServerConnection) -> None:
        self.connections += 1
        n = self.connections
        async for raw in ws:
            msg = json.loads(raw)
            self.received.append(msg)
            if msg.get("method") == "subscribe":
                sub = msg["subscription"]
                await ws.send(ack(sub["type"], sub["coin"]))
                if sub["type"] == "trades" and sub["coin"] == "BTC":
                    if n > 1 and self.silent_after_first:
                        continue
                    await ws.send(trades_msg((n, M0 + n, "B", 100, 1)))
                    if n == 1:
                        await ws.close()
                        return


async def _run_ws(rec: Recorder, server: FakeWsServer, until: Any) -> None:
    async with serve(server.handler, "127.0.0.1", 0) as srv:
        port = srv.sockets[0].getsockname()[1]
        rec.rc.ws_url = f"ws://127.0.0.1:{port}"
        rec.connect = lambda url: websockets.connect(url, ping_interval=None)
        task = asyncio.create_task(rec.ws_loop(rec.conns[0]))
        for _ in range(200):
            await asyncio.sleep(0.02)
            if until():
                break
        rec.stop_event.set()
        await asyncio.wait_for(task, 5)


async def test_ws_reconnects_resubscribes_and_pings(recorder: Recorder) -> None:
    rec = recorder
    server = FakeWsServer()
    await _run_ws(rec, server, lambda: server.connections >= 2 and any(
        m.get("method") == "ping" for m in server.received))  # fmt: skip
    subs = [m["subscription"] for m in server.received if m.get("method") == "subscribe"]
    # The server hung up after the first subscription; the reconnect subscribed everything.
    assert subs[1:] == rec.subscriptions()
    assert rec.reconnects >= 1
    assert {tr.tid for tr in rec._trade_buffer} >= {1, 2}


async def test_ws_dead_connection_is_replaced(recorder: Recorder) -> None:
    rec = recorder
    rec.rc.ping_interval_s = 10  # no pings, so the silent socket looks dead
    server = FakeWsServer(silent_after_first=True)

    async def silent(ws: ServerConnection) -> None:
        server.connections += 1
        async for raw in ws:
            server.received.append(json.loads(raw))

    server.handler = silent  # type: ignore[method-assign]
    await _run_ws(rec, server, lambda: server.connections >= 2)
    assert server.connections >= 2


async def test_bad_message_is_logged_not_fatal(
    recorder: Recorder, conn: sqlite3.Connection
) -> None:
    rec = recorder
    ack_all(rec, M0 + 20_000)
    rec.handle_message('{"channel": "trades", "data": [{"coin": "BTC"}]}', M0 + 30_000)
    rec.handle_message("not json", M0 + 30_000)
    rec.handle_message(trades_msg((1, M0 + 40_000, "B", 100, 1)), M0 + 40_100)
    assert rec.decode_errors == 2 and len(rec._trade_buffer) == 1
    rec.write_health(M0 + 41_000)
    detail = conn.execute("SELECT detail FROM stream_state WHERE stream = 'ws'").fetchone()[0]
    assert detail == "1/1 connected, 0 rotations, 2 undecodable messages"


async def test_tick_records_trade_addresses(recorder: Recorder, conn: sqlite3.Connection) -> None:
    rec = recorder
    ack_all(rec, M0 + 20_000)
    rec.handle_message(trades_msg((1, M0 + 30_000, "B", 100_000, 20)), M0 + 30_100)
    rec.handle_message(trades_msg((1, M0 + 30_000, "B", 100_000, 20)), M0 + 30_200)  # replay
    rec.tick(M0 + 31_000)
    rows = {r["address"]: r for r in conn.execute("SELECT * FROM addresses_seen")}
    assert rows["0xb"]["taker_ntl"] == 2e6 and rows["0xb"]["n_fills"] == 1
    assert rows["0xs"]["maker_ntl"] == 2e6
    assert rows["0xb"]["large"] == 1 and rows["0xb"]["probe_after_ts"] == 0


async def test_setup_refuses_over_budget(cfg: Config, conn: sqlite3.Connection) -> None:
    cfg.wallets.max_active = 5000
    http = httpx.AsyncClient(transport=httpx.MockTransport(FakeInfo()))
    async with InfoClient(WeightBudget(conn, 10_000), URL, http=http) as client:
        with pytest.raises(RuntimeError, match="exceeds the ceiling"):
            await Recorder(cfg, conn, client).setup()


async def test_liquidation_provider_hook(cfg: Config, conn: sqlite3.Connection) -> None:
    from hlflows.liquidations import providers

    class Fake:
        name = "fake"

        async def fetch(
            self, coins: set[str], start: int, end: int
        ) -> list[providers.ProviderLiquidation]:
            return [
                providers.ProviderLiquidation("e1", "BTC", start + 1, "0xa", "long", 1, 90_000),
                providers.ProviderLiquidation("e2", "BTC", start + 2, "0xa", "long", 2, 89_000),
            ]

        async def aclose(self) -> None:
            return None

    providers.ADAPTERS["fake"] = lambda c: Fake()
    try:
        cfg.liquidation_provider.name = "fake"
        with pytest.raises(ValueError, match="api_key"):
            providers.make_provider(cfg.liquidation_provider)
        cfg.liquidation_provider.api_key = "k"
        http = httpx.AsyncClient(transport=httpx.MockTransport(FakeInfo()))
        async with InfoClient(WeightBudget(conn, 10_000), URL, http=http) as client:
            rec = Recorder(cfg, conn, client, clock=Clock(M0))
            assert await rec.poll_provider({"BTC"}, "provider:fake") == 2
            assert await rec.poll_provider({"BTC"}, "provider:fake") == 2  # idempotent
        rows = conn.execute("SELECT id, quality, source FROM liquidations ORDER BY id").fetchall()
        assert [tuple(r) for r in rows] == [
            ("provider:e1:0xa", "external", "provider"),
            ("provider:e2:0xa", "external", "provider"),
        ]
    finally:
        del providers.ADAPTERS["fake"]
    cfg.liquidation_provider.name = "missing"
    with pytest.raises(ValueError, match="no liquidation provider adapter"):
        providers.make_provider(cfg.liquidation_provider)


async def _two_connections(cfg: Config, conn: sqlite3.Connection) -> AsyncIterator[Recorder]:
    cfg.recorder.ws_connections = 2
    http = httpx.AsyncClient(transport=httpx.MockTransport(FakeInfo()))
    async with InfoClient(WeightBudget(conn, 10_000), URL, http=http) as client:
        rec = Recorder(cfg, conn, client, clock=Clock(M0 + 20_000))
        await rec.setup()
        yield rec


@pytest.fixture
async def recorder2(cfg: Config, conn: sqlite3.Connection) -> AsyncIterator[Recorder]:
    async for rec in _two_connections(cfg, conn):
        yield rec


def minute_trades(start_tid: int, minute: int, n: int = 3) -> str:
    return trades_msg(*[(start_tid + i, minute + 10_000 * (i + 1), "B", 100, 1) for i in range(n)])


async def test_second_connection_covers_the_first_closing(
    recorder2: Recorder, conn: sqlite3.Connection
) -> None:
    """A rotation or server expiry on one connection costs nothing while another is live."""
    rec = recorder2
    M = [M0 + k * MINUTE_MS for k in range(8)]
    ack_all(rec, M0 + 20_000, cid=0)  # connection 0 live from M1
    for k in (1, 2):
        rec.handle_message(minute_trades(100 * k, M[k]), M[k] + 40_000, 0)
    ack_all(rec, M[2] + 30_000, cid=1)  # connection 1 joins; live from M3
    for k in (3, 4):  # both connections deliver the same trades
        rec.handle_message(minute_trades(100 * k, M[k]), M[k] + 40_000, 0)
        rec.handle_message(minute_trades(100 * k, M[k]), M[k] + 40_100, 1)
    rec.conns[0].last_msg_rx = M[4] + 40_000
    rec._on_disconnect(rec.conns[0])  # connection 0 is replaced / expires in minute 4
    for k in (5, 6):
        rec.handle_message(minute_trades(100 * k, M[k]), M[k] + 40_000, 1)
    rec.tick(M[6] + 6_000)

    rows = conn.execute(
        "SELECT ts, n, quality FROM bars WHERE market = 'BTC' AND res = '1m' AND ts >= ?"
        " ORDER BY ts", (M[1],)
    ).fetchall()  # fmt: skip
    assert [(r["ts"], r["n"], r["quality"]) for r in rows] == [(M[k], 3, None) for k in range(1, 6)]
    assert cov.get_coverage(conn, data.TAKER_STREAM, "BTC") == [(M[1], M[6])]
    assert conn.execute("SELECT COUNT(*) FROM gaps").fetchone()[0] == 0
    # Re-acking connection 0 while connection 1 is live records no gap either.
    ack_all(rec, M[6] + 50_000, cid=0)
    assert conn.execute("SELECT COUNT(*) FROM gaps").fetchone()[0] == 0


async def test_gap_when_every_connection_is_down(
    recorder2: Recorder, conn: sqlite3.Connection
) -> None:
    rec = recorder2
    M = [M0 + k * MINUTE_MS for k in range(10)]
    ack_all(rec, M0 + 20_000, cid=0)
    ack_all(rec, M0 + 25_000, cid=1)
    for k in (1, 2):
        rec.handle_message(minute_trades(100 * k, M[k]), M[k] + 40_000, 0)
    rec.tick(M[2] + 50_000)
    for c in rec.conns:
        c.last_msg_rx = M[2] + 40_000
        rec._on_disconnect(c)
    ack_all(rec, M[6] + 10_000, cid=1)
    gap = conn.execute(
        "SELECT start_ts, end_ts FROM gaps WHERE stream = 'taker_1m' AND market = 'BTC'"
    ).fetchone()
    assert (gap["start_ts"], gap["end_ts"]) == (M[2], M[7])


async def test_rotation_only_with_a_healthy_peer(recorder2: Recorder) -> None:
    rec = recorder2
    now = M0 + 10 * HOUR_MS
    old, young = rec.conns
    old.connected = young.connected = True
    old.started = now - 3 * HOUR_MS
    young.started = now - 10_000  # just connected: not trusted yet
    rec._check_stale(now)
    assert not old.rotating and not old.reconnect.is_set()
    young.started = now - 2 * MINUTE_MS
    rec._check_stale(now)
    assert old.rotating and old.reconnect.is_set()
    assert not young.rotating  # one rotation at a time, and it's young anyway
    young.started = now - 3 * HOUR_MS
    rec._check_stale(now)
    assert not young.rotating  # waits until the rotating peer is back and healthy


async def test_single_connection_never_rotates(recorder: Recorder) -> None:
    rec = recorder
    now = M0 + 10 * HOUR_MS
    rec.conns[0].connected = True
    rec.conns[0].started = now - 5 * HOUR_MS
    rec._check_stale(now)
    assert not rec.conns[0].rotating
