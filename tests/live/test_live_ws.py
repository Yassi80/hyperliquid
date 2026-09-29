"""Opt-in WebSocket smoke test: HL_LIVE=1 uv run pytest tests/live -m live"""

from __future__ import annotations

import asyncio
import json
import os

import pytest
import websockets

from hlflows.api import types as t

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(os.environ.get("HL_LIVE") != "1", reason="set HL_LIVE=1 to hit the API"),
]

WS_URL = "wss://api.hyperliquid.xyz/ws"


async def test_trades_and_asset_ctx_decode() -> None:
    seen: set[str] = set()
    async with websockets.connect(WS_URL) as ws:
        for sub in ({"type": "trades", "coin": "BTC"}, {"type": "activeAssetCtx", "coin": "BTC"}):
            await ws.send(json.dumps({"method": "subscribe", "subscription": sub}))
        await ws.send('{"method":"ping"}')

        async def read() -> None:
            while seen != {"subscriptionResponse", "trades", "activeAssetCtx", "pong"}:
                env = t.decode(await ws.recv(), t.WsEnvelope)
                seen.add(env.channel)
                if env.channel == "trades":
                    trades = t.decode(env.data, list[t.WsTrade])
                    assert trades and all(tr.side in ("A", "B") for tr in trades)
                    assert all(len(tr.users) == 2 for tr in trades)
                elif env.channel == "activeAssetCtx":
                    ctx = t.decode(env.data, t.WsActiveAssetCtx)
                    assert ctx.ctx.openInterest > 0 and ctx.ctx.premium is not None

        await asyncio.wait_for(read(), 60)
