"""Opt-in smoke tests against the real API: HL_LIVE=1 uv run pytest tests/live -m live"""

from __future__ import annotations

import os
import sqlite3

import pytest

from hlflows.api.client import InfoClient
from hlflows.budget import WeightBudget
from hlflows.config import MarketsConfig
from hlflows.markets import resolve_markets
from hlflows.timeutil import DAY_MS, HOUR_MS, now_ms

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(os.environ.get("HL_LIVE") != "1", reason="set HL_LIVE=1 to hit the API"),
]


async def test_markets_resolve(conn: sqlite3.Connection) -> None:
    async with InfoClient(WeightBudget(conn, 600)) as client:
        mac = await client.meta_and_asset_ctxs()
        spot = await client.spot_meta()
    markets, warnings = resolve_markets(MarketsConfig(), mac.meta, spot)
    assert warnings == []
    assert {m.market for m in markets} >= {"BTC", "ETH", "SOL", "HYPE", "HYPE/spot"}
    # Delisted assets can report premium: null; the configured coins must not.
    ctx = {a.name: c for a, c in zip(mac.meta.universe, mac.ctxs, strict=True)}
    assert all(ctx[c].premium is not None for c in ("BTC", "ETH", "SOL", "HYPE"))


async def test_candles_and_funding(conn: sqlite3.Connection) -> None:
    now = now_ms()
    async with InfoClient(WeightBudget(conn, 600)) as client:
        candles = await client.candles("BTC", "1h", now - DAY_MS, now)
        funding = await client.funding_history("BTC", now - DAY_MS)
    assert 20 <= len(candles) <= 25
    assert all(c.t % HOUR_MS == 0 for c in candles)
    assert 20 <= len(funding) <= 25


async def test_predicted_fundings(conn: sqlite3.Connection) -> None:
    async with InfoClient(WeightBudget(conn, 600)) as client:
        predicted = await client.predicted_fundings()
    venues = dict(predicted)["BTC"]
    names = {v for v, _ in venues}
    assert "HlPerp" in names
    hl = dict(venues)["HlPerp"]
    assert hl is not None and hl.fundingIntervalHours == 1


async def test_account_endpoints(conn: sqlite3.Connection) -> None:
    hlp_child = "0x010461c14e146ac35fe42271bdc1134ee31c703a"
    liquidator = "0x2e3d94f0562703b25c83308a05046ddaf9a8dd14"
    now = now_ms()
    async with InfoClient(WeightBudget(conn, 600)) as client:
        state = await client.clearinghouse_state(hlp_child)
        role = await client.user_role(liquidator)
        fills = await client.user_fills_by_time(liquidator, now - 90 * DAY_MS, now)
    assert state.marginSummary.accountValue > 0 and state.assetPositions
    assert role.role == "vault"
    # The liquidator's fills decode; any liquidation on them is a backstop.
    assert all(f.liquidation is None or f.liquidation.method == "backstop" for f in fills)
