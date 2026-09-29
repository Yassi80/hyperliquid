from __future__ import annotations

import sqlite3

import httpx
import pytest

from hlflows.api import types as t
from hlflows.api.client import ApiError, InfoClient, base_weight, surcharge
from hlflows.budget import WeightBudget
from hlflows.config import MarketsConfig
from hlflows.markets import resolve_markets
from tests.conftest import fixture_bytes
from tests.fake_api import URL, FakeInfo


def test_fixtures_decode() -> None:
    mac = t.decode(fixture_bytes("metaAndAssetCtxs.json"), t.MetaAndAssetCtxs)
    meta, ctxs = mac.meta, mac.ctxs
    assert {a.name for a in meta.universe} == {"BTC", "ETH", "SOL", "HYPE"}
    assert all(c.openInterest > 0 and c.premium is not None for c in ctxs)
    assert all(c.impactPxs is not None and len(c.impactPxs) == 2 for c in ctxs)
    candles = t.decode(fixture_bytes("candleSnapshot_BTC_1m.json"), list[t.Candle])
    assert all(c.t + 59_999 == c.T for c in candles)
    predicted = t.decode(fixture_bytes("predictedFundings.json"), t.PredictedFundings)
    venues = {v for _, vs in predicted for v, _ in vs}
    assert "HlPerp" in venues


def test_decode_rejects_missing_fields() -> None:
    import msgspec

    with pytest.raises(msgspec.ValidationError):
        t.decode(b'[{"coin": "BTC", "fundingRate": "0.1", "time": 1}]', list[t.FundingRecord])


def test_resolve_markets_from_fixtures() -> None:
    meta = t.decode(fixture_bytes("metaAndAssetCtxs.json"), t.MetaAndAssetCtxs).meta
    spot = t.decode(fixture_bytes("spotMeta.json"), t.SpotMeta)
    markets, warnings = resolve_markets(MarketsConfig(), meta, spot)
    assert warnings == []
    by_id = {m.market: m for m in markets}
    assert by_id["HYPE/spot"].hl_name == "@107" and by_id["HYPE/spot"].significance == "normal"
    assert by_id["BTC/spot"].hl_name == "@142" and by_id["BTC/spot"].significance == "low"
    assert by_id["ETH"].kind == "perp" and by_id["ETH"].hl_name == "ETH"


def test_resolve_markets_warns_on_unknown() -> None:
    meta = t.decode(fixture_bytes("metaAndAssetCtxs.json"), t.MetaAndAssetCtxs).meta
    spot = t.decode(fixture_bytes("spotMeta.json"), t.SpotMeta)
    cfg = MarketsConfig(coins=["BTC", "DOGE"], spot_tokens={"DOGE": "UDOGE"})
    markets, warnings = resolve_markets(cfg, meta, spot)
    assert [m.market for m in markets] == ["BTC"]
    assert len(warnings) == 2


def test_weights() -> None:
    assert base_weight("clearinghouseState") == 2
    assert base_weight("candleSnapshot") == 20
    assert base_weight("userRole") == 60
    assert surcharge("candleSnapshot", 5000) == 84
    assert surcharge("fundingHistory", 500) == 25
    assert surcharge("metaAndAssetCtxs", 500) == 0


def _client(conn: sqlite3.Connection, fake: FakeInfo, retries: int = 3) -> InfoClient:
    http = httpx.AsyncClient(transport=httpx.MockTransport(fake))
    return InfoClient(WeightBudget(conn, 1000), URL, max_retries=retries, http=http)


async def test_client_charges_base_and_surcharge(conn: sqlite3.Connection) -> None:
    fake = FakeInfo()
    async with _client(conn, fake) as client:
        recs = await client.funding_history("HYPE", 0)
    assert recs
    assert client.budget.used() == 20 + surcharge("fundingHistory", len(recs))


async def test_client_retries_429(
    conn: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def no_sleep(_: float) -> None:
        return None

    monkeypatch.setattr("hlflows.api.client.asyncio.sleep", no_sleep)
    fake = FakeInfo()
    responses = iter([httpx.Response(429), httpx.Response(502)])
    fake.overrides["spotMeta"] = lambda body: next(
        responses, httpx.Response(200, content=fixture_bytes("spotMeta.json"))
    )
    async with _client(conn, fake) as client:
        spot = await client.spot_meta()
    assert spot.universe and fake.count("spotMeta") == 3


async def test_client_gives_up(conn: sqlite3.Connection, monkeypatch: pytest.MonkeyPatch) -> None:
    async def no_sleep(_: float) -> None:
        return None

    monkeypatch.setattr("hlflows.api.client.asyncio.sleep", no_sleep)
    fake = FakeInfo()
    fake.overrides["spotMeta"] = lambda body: httpx.Response(503)
    async with _client(conn, fake, retries=2) as client:
        with pytest.raises(ApiError):
            await client.spot_meta()
    assert fake.count("spotMeta") == 3
