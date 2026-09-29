"""Adapter interface for a paid provider with a global Hyperliquid liquidation stream
(e.g. GoldRush or Bitquery). Off by default; nothing here calls a paid API unless an
adapter is registered and `[liquidation_provider]` names it with an API key.

To add one: implement `LiquidationProvider` (fetch liquidations in a time window), wrap
it in a factory taking the config, and add it to `ADAPTERS`. The recorder then polls it
every `poll_s` seconds and stores results with source 'provider', quality 'external'.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

from hlflows.config import LiquidationProviderConfig


@dataclass(frozen=True)
class ProviderLiquidation:
    id: str  # provider's stable event id (used for idempotent inserts)
    coin: str
    ts: int  # UTC ms
    address: str
    side: str  # liquidated position: "long" | "short"
    sz: float  # coin units
    px: float
    method: str = "unknown"  # "market" | "backstop" | "unknown"


class LiquidationProvider(Protocol):
    name: str

    async def fetch(self, coins: set[str], start: int, end: int) -> list[ProviderLiquidation]:
        """Liquidations in configured coins with start <= ts < end."""
        ...

    async def aclose(self) -> None: ...


ADAPTERS: dict[str, Callable[[LiquidationProviderConfig], LiquidationProvider]] = {}


def make_provider(cfg: LiquidationProviderConfig) -> LiquidationProvider | None:
    if not cfg.name:
        return None
    if not cfg.api_key:
        raise ValueError(f"liquidation_provider.name = {cfg.name!r} but no api_key is set")
    factory = ADAPTERS.get(cfg.name)
    if factory is None:
        known = ", ".join(sorted(ADAPTERS)) or "none registered"
        raise ValueError(
            f"no liquidation provider adapter named {cfg.name!r} ({known}); see "
            "hlflows/liquidations/providers.py for how to add one"
        )
    return factory(cfg)
