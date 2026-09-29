"""Refresh the recorded API fixtures in tests/fixtures from the live API.

Payloads are trimmed to the configured coins to keep fixtures small; their structure is
left untouched. Run: uv run python scripts/record_fixtures.py
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import httpx

URL = "https://api.hyperliquid.xyz/info"
OUT = Path(__file__).resolve().parent.parent / "tests" / "fixtures"
COINS = {"BTC", "ETH", "SOL", "HYPE"}
SPOT_BASES = {"UBTC", "UETH", "USOL", "HYPE", "USDC"}
HLP_CHILD = "0x010461c14e146ac35fe42271bdc1134ee31c703a"
LIQUIDATOR = "0x2e3d94f0562703b25c83308a05046ddaf9a8dd14"


def post(body: dict[str, Any]) -> Any:
    resp = httpx.post(URL, json=body, timeout=30)
    resp.raise_for_status()
    return resp.json()


def save(name: str, payload: Any) -> None:
    (OUT / name).write_text(json.dumps(payload, indent=1) + "\n")
    print("saved", name)


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    now = int(time.time() * 1000)

    meta, ctxs = post({"type": "metaAndAssetCtxs"})
    keep = [i for i, a in enumerate(meta["universe"]) if a["name"] in COINS]
    meta["universe"] = [meta["universe"][i] for i in keep]
    save("metaAndAssetCtxs.json", [meta, [ctxs[i] for i in keep]])

    spot = post({"type": "spotMeta"})
    tokens = {t["index"]: t["name"] for t in spot["tokens"]}
    spot["universe"] = [
        p for p in spot["universe"] if all(tokens.get(i) in SPOT_BASES for i in p["tokens"])
    ]
    used = {i for p in spot["universe"] for i in p["tokens"]}
    spot["tokens"] = [t for t in spot["tokens"] if t["index"] in used]
    save("spotMeta.json", spot)

    save(
        "candleSnapshot_ETH_1h.json",
        post(
            {
                "type": "candleSnapshot",
                "req": {
                    "coin": "ETH",
                    "interval": "1h",
                    "startTime": now - 24 * 3600_000,
                    "endTime": now,
                },
            }
        ),
    )
    save(
        "candleSnapshot_BTC_1m.json",
        post(
            {
                "type": "candleSnapshot",
                "req": {
                    "coin": "BTC",
                    "interval": "1m",
                    "startTime": now - 60 * 60_000,
                    "endTime": now,
                },
            }
        ),
    )
    save(
        "fundingHistory_HYPE.json",
        post({"type": "fundingHistory", "coin": "HYPE", "startTime": now - 20 * 86400_000}),
    )

    predicted = post({"type": "predictedFundings"})
    save("predictedFundings.json", [row for row in predicted if row[0] in COINS])

    # An HLP child account: many positions, mixed sides. Trimmed to the configured coins
    # plus two others so filtering is exercised.
    state = post({"type": "clearinghouseState", "user": HLP_CHILD})
    state["assetPositions"] = [
        p for p in state["assetPositions"] if p["position"]["coin"] in COINS | {"DOGE", "AVAX"}
    ]
    save("clearinghouseState.json", state)

    # The HLP liquidator: keep a few fills including backstop liquidations.
    fills = post({"type": "userFills", "user": LIQUIDATOR})
    liq = [f for f in fills if f.get("liquidation")][:2]
    save("userFills_liquidator.json", liq + [f for f in fills if not f.get("liquidation")][:3])

    save("userRole_vault.json", post({"type": "userRole", "user": LIQUIDATOR}))


if __name__ == "__main__":
    main()
