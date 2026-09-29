"""REST client for the public `POST /info` endpoint, with weight budgeting and retries."""

from __future__ import annotations

import asyncio
import logging
import math
from typing import Any

import httpx

from hlflows.api import types as t
from hlflows.budget import WeightBudget, backoff_delays

log = logging.getLogger(__name__)

# Per-request base weights, from the Hyperliquid rate-limit docs (verified 2026-09-27).
LIGHT_REQUESTS = {
    "l2Book",
    "allMids",
    "clearinghouseState",
    "orderStatus",
    "spotClearinghouseState",
    "exchangeStatus",
}
HEAVY_REQUESTS = {"userRole": 60}
DEFAULT_WEIGHT = 20
# Extra weight per N items returned.
ITEM_SURCHARGE = {
    "candleSnapshot": 60,
    "recentTrades": 20,
    "historicalOrders": 20,
    "userFills": 20,
    "userFillsByTime": 20,
    "fundingHistory": 20,
    "userFunding": 20,
    "nonUserFundingUpdates": 20,
    "twapHistory": 20,
    "userTwapSliceFills": 20,
    "userTwapSliceFillsByTime": 20,
    "delegatorHistory": 20,
    "delegatorRewards": 20,
    "validatorStats": 20,
}

RETRY_STATUS = {429, 500, 502, 503, 504}


def base_weight(request_type: str) -> int:
    if request_type in LIGHT_REQUESTS:
        return 2
    return HEAVY_REQUESTS.get(request_type, DEFAULT_WEIGHT)


def surcharge(request_type: str, n_items: int) -> int:
    per = ITEM_SURCHARGE.get(request_type)
    return math.ceil(n_items / per) if per else 0


class ApiError(RuntimeError):
    pass


class InfoClient:
    def __init__(
        self,
        budget: WeightBudget,
        url: str = "https://api.hyperliquid.xyz/info",
        timeout_s: float = 20.0,
        max_retries: int = 6,
        http: httpx.AsyncClient | None = None,
    ) -> None:
        self.budget = budget
        self.url = url
        self.max_retries = max_retries
        self._http = http or httpx.AsyncClient(timeout=timeout_s)

    async def aclose(self) -> None:
        await self._http.aclose()

    async def __aenter__(self) -> InfoClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    async def request[T](self, body: dict[str, Any], type_: type[T]) -> T:
        request_type = body["type"]
        delays = backoff_delays()
        for attempt in range(self.max_retries + 1):
            await self.budget.acquire(base_weight(request_type), request_type)
            try:
                resp = await self._http.post(self.url, json=body)
            except httpx.TransportError as exc:
                if attempt == self.max_retries:
                    raise ApiError(f"{request_type}: {exc!r}") from exc
                delay = next(delays)
                log.warning("%s: %r, retrying in %.1fs", request_type, exc, delay)
                await asyncio.sleep(delay)
                continue
            if resp.status_code in RETRY_STATUS and attempt < self.max_retries:
                delay = next(delays)
                log.warning("%s: HTTP %s, retrying in %.1fs", request_type, resp.status_code, delay)
                await asyncio.sleep(delay)
                continue
            if resp.status_code != 200:
                raise ApiError(f"{request_type}: HTTP {resp.status_code}: {resp.text[:200]}")
            result = t.decode(resp.content, type_)
            if isinstance(result, list):
                self.budget.charge(surcharge(request_type, len(result)), request_type)
            return result
        raise AssertionError("unreachable")

    async def meta_and_asset_ctxs(self, dex: str = "") -> t.MetaAndAssetCtxs:
        body: dict[str, Any] = {"type": "metaAndAssetCtxs"}
        if dex:
            body["dex"] = dex
        return await self.request(body, t.MetaAndAssetCtxs)

    async def spot_meta(self) -> t.SpotMeta:
        return await self.request({"type": "spotMeta"}, t.SpotMeta)

    async def candles(self, coin: str, interval: str, start: int, end: int) -> list[t.Candle]:
        body = {
            "type": "candleSnapshot",
            "req": {"coin": coin, "interval": interval, "startTime": start, "endTime": end},
        }
        return await self.request(body, list[t.Candle])

    async def funding_history(
        self, coin: str, start: int, end: int | None = None
    ) -> list[t.FundingRecord]:
        body: dict[str, Any] = {"type": "fundingHistory", "coin": coin, "startTime": start}
        if end is not None:
            body["endTime"] = end
        return await self.request(body, list[t.FundingRecord])

    async def predicted_fundings(self) -> t.PredictedFundings:
        return await self.request({"type": "predictedFundings"}, t.PredictedFundings)

    async def clearinghouse_state(self, user: str, dex: str = "") -> t.ClearinghouseState:
        body: dict[str, Any] = {"type": "clearinghouseState", "user": user}
        if dex:
            body["dex"] = dex
        return await self.request(body, t.ClearinghouseState)

    async def user_fills_by_time(self, user: str, start: int, end: int) -> list[t.UserFill]:
        body = {"type": "userFillsByTime", "user": user, "startTime": start, "endTime": end}
        return await self.request(body, list[t.UserFill])

    async def user_role(self, user: str) -> t.UserRole:
        return await self.request({"type": "userRole", "user": user}, t.UserRole)
