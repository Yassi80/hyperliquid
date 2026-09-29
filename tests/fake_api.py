"""An in-process stand-in for POST /info that serves the recorded fixtures."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

import httpx

from tests.conftest import fixture_json

URL = "https://api.hyperliquid.xyz/info"


class FakeInfo:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.overrides: dict[str, Callable[[dict[str, Any]], httpx.Response]] = {}

    def __call__(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        self.calls.append(body)
        kind = body["type"]
        if kind in self.overrides:
            return self.overrides[kind](body)
        if kind == "metaAndAssetCtxs":
            return httpx.Response(200, json=fixture_json("metaAndAssetCtxs.json"))
        if kind == "spotMeta":
            return httpx.Response(200, json=fixture_json("spotMeta.json"))
        if kind == "predictedFundings":
            return httpx.Response(200, json=fixture_json("predictedFundings.json"))
        if kind == "candleSnapshot":
            req = body["req"]
            name = {"1h": "candleSnapshot_ETH_1h.json", "1m": "candleSnapshot_BTC_1m.json"}.get(
                req["interval"]
            )
            candles = fixture_json(name) if name else []
            out = [
                {**c, "s": req["coin"]}
                for c in candles
                if req["startTime"] <= c["t"] <= req["endTime"]
            ]
            return httpx.Response(200, json=out)
        if kind == "fundingHistory":
            recs = fixture_json("fundingHistory_HYPE.json")
            end = body.get("endTime", 1 << 62)
            out = [
                {**r, "coin": body["coin"]} for r in recs if body["startTime"] <= r["time"] <= end
            ]
            return httpx.Response(200, json=out[:500])
        return httpx.Response(400, text=f"unexpected request {kind}")

    def count(self, kind: str) -> int:
        return sum(1 for c in self.calls if c["type"] == kind)
