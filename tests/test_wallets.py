from __future__ import annotations

import sqlite3
from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest

from hlflows import coverage as cov
from hlflows import store
from hlflows.api import types as t
from hlflows.api.client import InfoClient
from hlflows.budget import WeightBudget
from hlflows.config import Config, WalletsConfig
from hlflows.timeutil import DAY_MS, HOUR_MS, MINUTE_MS
from hlflows.wallets import store as ws
from hlflows.wallets.tracker import WalletTracker
from tests.conftest import fixture_bytes, fixture_json
from tests.fake_api import URL, FakeInfo

NOW = 1_790_000_000_000


class Clock:
    def __init__(self, t: int) -> None:
        self.t = t

    def __call__(self) -> int:
        return self.t


def state(*positions: tuple[str, float, float, float | None], account: float = 1e6) -> Any:
    """clearinghouseState JSON from (coin, szi, position_value, liq_px) tuples."""
    return {
        "assetPositions": [
            {"type": "oneWay", "position": {
                "coin": c, "szi": str(szi), "leverage": {"type": "cross", "value": 10},
                "entryPx": "1", "positionValue": str(val), "unrealizedPnl": "0",
                "marginUsed": str(val / 10),
                "liquidationPx": None if liq is None else str(liq)}}
            for c, szi, val, liq in positions
        ],
        "marginSummary": {"accountValue": str(account), "totalNtlPos": "0", "totalMarginUsed": "0"},
        "crossMarginSummary": {"accountValue": str(account), "totalNtlPos": str(2 * account),
                               "totalMarginUsed": "0"},
        "time": NOW,
    }  # fmt: skip


class Accounts:
    """clearinghouseState / userRole / userFillsByTime per address."""

    def __init__(self, fake: FakeInfo) -> None:
        self.states: dict[str, Any] = {}
        self.roles: dict[str, str] = {}
        self.fills: dict[str, list[Any]] = {}
        fake.overrides["clearinghouseState"] = lambda b: httpx.Response(
            200, json=self.states.get(b["user"], state())
        )
        fake.overrides["userRole"] = lambda b: httpx.Response(
            200, json={"role": self.roles.get(b["user"], "user")}
        )
        fake.overrides["userFillsByTime"] = lambda b: httpx.Response(
            200, json=self.fills.get(b["user"], []))  # fmt: skip


@pytest.fixture
def fake() -> FakeInfo:
    return FakeInfo()


@pytest.fixture
def accounts(fake: FakeInfo) -> Accounts:
    return Accounts(fake)


@pytest.fixture
async def tracker(conn: sqlite3.Connection, fake: FakeInfo) -> AsyncIterator[WalletTracker]:
    cfg = Config(wallets=WalletsConfig(max_active=2, probe_weight_per_min=10_000,
                                       manual=["0xmanual"]))  # fmt: skip
    http = httpx.AsyncClient(transport=httpx.MockTransport(fake))
    async with InfoClient(WeightBudget(conn, 100_000), URL, http=http) as client:
        yield WalletTracker(cfg, conn, client, clock=Clock(NOW))


def trade(tid: int, side: str, px: float, sz: float, buyer: str, seller: str) -> t.WsTrade:
    return t.WsTrade("BTC", side, px, sz, NOW + tid, "0x", tid, (buyer, seller))


def seen(conn: sqlite3.Connection, *addresses: str, large: bool = False) -> None:
    activity = {a: ws.AddressActivity(NOW, NOW, 1, 1e5, 0, 1e5, large) for a in addresses}
    with store.transaction(conn):
        ws.upsert_activity(conn, activity, NOW)


def status(conn: sqlite3.Connection, address: str) -> str | None:
    row = ws.get_wallet(conn, address)
    return None if row is None else str(row["status"])


def test_activity_assigns_taker_and_maker() -> None:
    trades = [("BTC", trade(1, "B", 100_000, 20, "0xbuyer", "0xseller")),
              ("BTC", trade(2, "A", 100_000, 0.1, "0xbuyer", "0xseller"))]  # fmt: skip
    a = ws.aggregate_activity(trades, {"BTC": "BTC"}, {"BTC": 1e6})
    assert a["0xbuyer"].taker_ntl == 2e6 and a["0xbuyer"].maker_ntl == 1e4
    assert a["0xseller"].taker_ntl == 1e4 and a["0xseller"].maker_ntl == 2e6
    assert a["0xbuyer"].large and a["0xbuyer"].n == 2 and a["0xbuyer"].max_ntl == 2e6


def test_large_trade_requeues_unless_probed_recently(conn: sqlite3.Connection) -> None:
    seen(conn, "0xa")
    with store.transaction(conn):
        ws.record_probe(conn, "0xa", NOW, 0, NOW + 7 * DAY_MS)
    seen(conn, "0xa", large=True)  # probed just now: stays scheduled
    assert ws.get_activity(conn, "0xa")["probe_after_ts"] == NOW + 7 * DAY_MS  # type: ignore[index]
    with store.transaction(conn):
        ws.record_probe(conn, "0xa", NOW - 2 * HOUR_MS, 0, NOW + 7 * DAY_MS)
    seen(conn, "0xa", large=True)
    row = ws.get_activity(conn, "0xa")
    assert row is not None and row["probe_after_ts"] == 0 and row["n_fills"] == 3


async def test_probe_outcomes(
    tracker: WalletTracker, accounts: Accounts, conn: sqlite3.Connection
) -> None:
    seen(conn, "0xbig", "0xsmall", "0xvault", "0xbigger", large=True)
    accounts.states["0xbig"] = state(("BTC", 10, 1e6, 90_000), ("DOGE", 1e6, 5e6, 0.1))
    accounts.states["0xsmall"] = state(("BTC", 1, 1e5, 90_000))  # below the $500k BTC bar
    accounts.states["0xbigger"] = state(("ETH", -500, 2e6, 3_000))
    tracker.vaults.add("0xvault")
    accounts.states["0xvault"] = state(("SOL", 1e5, 1e7, None))

    results = {r.address: r for r in await tracker.probe_due(limit=10)}
    assert results["0xsmall"].outcome == "not-qualified"
    assert results["0xbig"].ntl == 1e6  # DOGE isn't a configured coin
    assert results["0xvault"].outcome == "vault" and status(conn, "0xvault") == "dormant"
    # max_active=2: the first qualifying wallet fills the slot next to nothing else, then
    # the bigger one displaces the smaller.
    assert status(conn, "0xbigger") == "active"
    assert {results["0xbig"].outcome, results["0xbigger"].outcome} <= {
        "activated",
        "displaced-smaller",
    }
    small = ws.get_activity(conn, "0xsmall")
    assert small is not None and small["probe_after_ts"] == NOW + 7 * DAY_MS
    assert await tracker.probe_due(limit=10) == []  # nothing due any more


async def test_capacity_displaces_smallest_discovered(
    tracker: WalletTracker, accounts: Accounts, conn: sqlite3.Connection
) -> None:
    tracker.sync_manual()  # 0xmanual takes one of the two slots
    seen(conn, "0xa")
    accounts.states["0xa"] = state(("BTC", 10, 6e5, 1))
    assert (await tracker.probe("0xa")).outcome == "activated"
    seen(conn, "0xb")
    accounts.states["0xb"] = state(("BTC", 10, 5.5e5, 1))
    assert (await tracker.probe("0xb")).outcome == "full"  # smaller than 0xa
    accounts.states["0xc"] = state(("BTC", 10, 9e5, 1))
    seen(conn, "0xc")
    assert (await tracker.probe("0xc")).outcome == "displaced-smaller"
    assert status(conn, "0xa") == "dormant" and status(conn, "0xc") == "active"
    assert status(conn, "0xmanual") == "active"


async def test_market_maker_is_tagged(
    tracker: WalletTracker, accounts: Accounts, conn: sqlite3.Connection
) -> None:
    with store.transaction(conn):
        ws.upsert_activity(conn, {"0xmm": ws.AddressActivity(NOW, NOW, 5000, 1e6, 9e7, 1e5)}, NOW)
    accounts.states["0xmm"] = state(("BTC", 10, 1e6, 1))  # $1M vs $91M traded
    assert (await tracker.probe("0xmm")).outcome == "mm"
    row = ws.get_wallet(conn, "0xmm")
    assert row is not None and row["status"] == "dormant" and row["tags"] == "mm"


async def test_poll_round_stores_positions_and_coverage(
    tracker: WalletTracker, accounts: Accounts, conn: sqlite3.Connection
) -> None:
    tracker.sync_manual()
    accounts.states["0xmanual"] = state(("BTC", 2, 2e5, 80_000), ("DOGE", 1, 1, None),
                                        ("ETH", -3, 9e3, None), account=5e4)  # fmt: skip
    round_ts = await tracker.poll_round()
    rows = conn.execute("SELECT coin, szi, eff_lev, liq_px FROM positions").fetchall()
    assert {r["coin"]: r["szi"] for r in rows} == {"BTC": 2, "ETH": -3}
    assert rows[0]["eff_lev"] == pytest.approx(2.0)  # cross: 2 x account / account
    assert cov.get_coverage(conn, "positions", "watchlist") == [(round_ts, round_ts + 120_000)]
    # Manual wallets never go dormant even without a qualifying position.
    assert isinstance(tracker.clock, Clock)
    tracker.clock.t += 2 * DAY_MS
    await tracker.poll_round()
    assert status(conn, "0xmanual") == "active"
    gap = conn.execute("SELECT recoverable FROM gaps WHERE stream = 'positions'").fetchone()
    assert gap[0] == "none"  # two days without rounds


async def test_discovered_wallet_goes_dormant(
    tracker: WalletTracker, accounts: Accounts, conn: sqlite3.Connection
) -> None:
    seen(conn, "0xa")
    accounts.states["0xa"] = state(("BTC", 10, 6e5, 1))
    await tracker.probe("0xa")
    accounts.states["0xa"] = state()  # closed everything
    assert isinstance(tracker.clock, Clock)
    tracker.clock.t += HOUR_MS
    await tracker.poll_round()
    assert status(conn, "0xa") == "active"  # not yet 24h
    tracker.clock.t += 24 * HOUR_MS
    await tracker.poll_round()
    assert status(conn, "0xa") == "dormant"


async def test_liquidation_check_after_shrink_near_liq_price(
    tracker: WalletTracker, accounts: Accounts, conn: sqlite3.Connection
) -> None:
    tracker.sync_manual()
    accounts.states["0xmanual"] = state(("BTC", 1, 1e5, 98_000))
    await tracker.poll_round()
    with store.transaction(conn):  # the mark fell to within 3% of the liquidation price
        conn.execute(
            "INSERT INTO asset_ctx (coin, ts, oi, mark_px, oracle_px, funding_1h, source)"
            " VALUES ('BTC', ?, 1, 100000, 100000, 0, 'ws')",
            (NOW,),
        )
    accounts.states["0xmanual"] = state()  # position gone
    fill = {"coin": "BTC", "px": "98000", "sz": "1", "side": "A", "time": NOW + 60_000,
            "dir": "Close Long", "tid": 7, "hash": "0x", "crossed": True,
            "liquidation": {"markPx": "98000", "method": "market",
                            "liquidatedUser": "0xmanual"}}  # fmt: skip
    accounts.fills["0xmanual"] = [fill]
    assert isinstance(tracker.clock, Clock)
    tracker.clock.t += 2 * MINUTE_MS
    await tracker.poll_round()
    liq = conn.execute("SELECT * FROM liquidations").fetchone()
    assert (liq["side"], liq["method"], liq["source"], liq["ntl"]) == (
        "long",
        "market",
        "watched_fills",
        98_000,
    )


async def test_liquidator_fills_are_backstop_liquidations(
    tracker: WalletTracker, accounts: Accounts, conn: sqlite3.Connection
) -> None:
    liquidator = next(iter(tracker.liquidators))
    accounts.fills[liquidator] = fixture_json("userFills_liquidator.json")
    tracker.coins |= {"HMSTR", "PURR"}  # the recorded backstops were on small coins
    assert await tracker.poll_liquidators() == 2
    rows = conn.execute("SELECT * FROM liquidations ORDER BY ts").fetchall()
    assert {r["method"] for r in rows} == {"backstop"}
    assert {r["side"] for r in rows} == {"short"}  # "Liquidated Isolated Short"
    assert all(r["address"] != liquidator for r in rows)
    assert cov.get_coverage(conn, "liquidator_fills", liquidator)


async def test_role_check_tags_vaults(
    tracker: WalletTracker, accounts: Accounts, conn: sqlite3.Connection
) -> None:
    seen(conn, "0xv")
    accounts.states["0xv"] = state(("BTC", 10, 6e5, 1))
    accounts.roles["0xv"] = "vault"
    await tracker.probe("0xv")
    assert await tracker.check_roles() == 1
    row = ws.get_wallet(conn, "0xv")
    assert row is not None and row["role"] == "vault" and row["status"] == "dormant"
    assert row["tags"] == "vault"


async def test_probe_sub_budget(
    tracker: WalletTracker, accounts: Accounts, conn: sqlite3.Connection
) -> None:
    tracker.w.probe_weight_per_min = 10  # 5 probes of weight 2
    seen(conn, *[f"0x{i}" for i in range(20)])
    assert len(await tracker.probe_due()) == 5
    assert await tracker.probe_due() == []
    assert isinstance(tracker.clock, Clock)
    tracker.clock.t += MINUTE_MS
    assert len(await tracker.probe_due()) == 5


def test_fixture_decodes() -> None:
    s = t.decode(fixture_bytes("clearinghouseState.json"), t.ClearinghouseState)
    rows = ws.position_rows(1, "0xhlp", s, {"BTC", "ETH", "SOL", "HYPE"})
    assert rows and all(r.coin in {"BTC", "ETH", "SOL", "HYPE"} for r in rows)
    assert all(r.eff_lev is not None and r.eff_lev > 0 for r in rows)


async def test_market_maker_detected_after_activation(
    tracker: WalletTracker, accounts: Accounts, conn: sqlite3.Connection
) -> None:
    seen(conn, "0xmm")  # one fill seen so far: can't tell yet
    accounts.states["0xmm"] = state(("BTC", 10, 1e6, 1))
    assert (await tracker.probe("0xmm")).outcome == "activated"
    with store.transaction(conn):  # the recorder keeps seeing it make markets
        ws.upsert_activity(conn, {"0xmm": ws.AddressActivity(NOW, NOW, 5000, 1e6, 9e7, 1e5)}, NOW)
    await tracker.poll_round()
    row = ws.get_wallet(conn, "0xmm")
    assert row is not None and row["status"] == "dormant" and row["tags"] == "mm"
