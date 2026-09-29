"""Typed Hyperliquid payloads.

The API sends numbers as strings; payloads are decoded with `strict=False` so msgspec
converts them to floats while still rejecting missing or wrongly-typed fields.
Unknown extra fields are ignored so additive API changes don't break decoding.
"""

from __future__ import annotations

from typing import Any

import msgspec


def decode[T](data: bytes | str, type_: type[T]) -> T:
    return msgspec.json.decode(data, type=type_, strict=False)


def convert[T](obj: Any, type_: type[T]) -> T:
    return msgspec.convert(obj, type=type_, strict=False)


class PerpAsset(msgspec.Struct):
    name: str
    szDecimals: int
    maxLeverage: int
    isDelisted: bool = False


class PerpMeta(msgspec.Struct):
    universe: list[PerpAsset]


class PerpAssetCtx(msgspec.Struct):
    funding: float
    openInterest: float
    oraclePx: float
    markPx: float
    dayNtlVlm: float
    prevDayPx: float
    premium: float | None = None
    midPx: float | None = None
    impactPxs: list[float] | None = None
    dayBaseVlm: float | None = None


class MetaAndAssetCtxs(msgspec.Struct, array_like=True):
    """`[meta, ctxs]`: ctxs[i] is the context of meta.universe[i]."""

    meta: PerpMeta
    ctxs: list[PerpAssetCtx]


class SpotToken(msgspec.Struct):
    name: str
    index: int
    szDecimals: int


class SpotPair(msgspec.Struct):
    name: str
    tokens: list[int]
    index: int
    isCanonical: bool = False


class SpotMeta(msgspec.Struct):
    tokens: list[SpotToken]
    universe: list[SpotPair]


class Candle(msgspec.Struct):
    t: int  # open time
    T: int  # close time (inclusive, t + interval - 1)
    s: str
    i: str
    o: float
    h: float
    l: float  # noqa: E741
    c: float
    v: float  # base volume
    n: int


class FundingRecord(msgspec.Struct):
    coin: str
    fundingRate: float  # per hour
    premium: float  # 8h-scale premium index
    time: int


class VenueFunding(msgspec.Struct):
    fundingRate: float  # per funding interval
    nextFundingTime: int
    fundingIntervalHours: float = 8.0


# [[coin, [[venue, VenueFunding | null], ...]], ...]
PredictedFundings = list[tuple[str, list[tuple[str, VenueFunding | None]]]]


# --- WebSocket ---------------------------------------------------------------------


class WsEnvelope(msgspec.Struct):
    channel: str
    data: msgspec.Raw = msgspec.Raw(b"null")


class WsTrade(msgspec.Struct):
    coin: str
    side: str  # taker side: "B" = buy, "A" = sell (verified live 2026-09-27)
    px: float
    sz: float
    time: int
    hash: str
    tid: int
    users: tuple[str, str]  # (buyer, seller)


class WsActiveAssetCtx(msgspec.Struct):
    coin: str
    ctx: PerpAssetCtx


# --- Accounts ---------------------------------------------------------------------


class Leverage(msgspec.Struct):
    type: str  # "cross" | "isolated"
    value: float


class Position(msgspec.Struct):
    coin: str
    szi: float  # signed size: > 0 long, < 0 short
    leverage: Leverage
    entryPx: float
    positionValue: float  # |szi| * mark
    unrealizedPnl: float
    marginUsed: float
    liquidationPx: float | None = None  # None when the account can't be liquidated


class AssetPosition(msgspec.Struct):
    position: Position
    type: str = "oneWay"


class MarginSummary(msgspec.Struct):
    accountValue: float
    totalNtlPos: float
    totalMarginUsed: float


class ClearinghouseState(msgspec.Struct):
    assetPositions: list[AssetPosition]
    marginSummary: MarginSummary
    crossMarginSummary: MarginSummary
    time: int


class FillLiquidation(msgspec.Struct):
    markPx: float
    method: str  # "market" | "backstop"
    liquidatedUser: str | None = None


class UserFill(msgspec.Struct):
    coin: str
    px: float
    sz: float
    side: str  # "B" | "A"
    time: int
    dir: str
    tid: int
    hash: str
    crossed: bool
    liquidation: FillLiquidation | None = None


class UserRole(msgspec.Struct):
    role: str  # "user" | "vault" | "subAccount" | "agent" | "missing"
