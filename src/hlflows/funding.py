"""Funding normalization and percentiles.

Hyperliquid (verified against docs and live fundingHistory, 2026-09-27):
    F_8h = P + clamp(I - P, -0.0005, +0.0005),  I = 0.0001 (0.01% per 8h)
    paid hourly at F_8h / 8, capped at 4% per hour.
P is the 8h-scale premium index. Whenever -0.0004 <= P <= 0.0006, funding is pinned at
exactly I / 8 = 0.00125% per hour, so the premium can't be recovered from funding; it is
stored straight from the API instead.
"""

from __future__ import annotations

import bisect
from collections.abc import Sequence
from dataclasses import dataclass

INTEREST_8H = 0.0001
INTEREST_1H = INTEREST_8H / 8
CLAMP_8H = 0.0005
CAP_1H = 0.04
PIN_TOLERANCE = 1e-10


def funding_1h_from_premium(premium_8h: float) -> float:
    f_8h = premium_8h + min(CLAMP_8H, max(-CLAMP_8H, INTEREST_8H - premium_8h))
    return max(-CAP_1H, min(CAP_1H, f_8h / 8))


def to_8h(rate_1h: float) -> float:
    return rate_1h * 8


def venue_rate_to_8h(rate: float, interval_h: float) -> float:
    """Normalize a per-interval venue rate (1h on Hyperliquid, usually 8h on CEXs) to 8h."""
    return rate * 8 / interval_h


def is_pinned(rate_1h: float) -> bool:
    return abs(rate_1h - INTEREST_1H) <= PIN_TOLERANCE


def midrank_percentile(value: float, window: Sequence[float]) -> float | None:
    """Percentile of `value` within `window` (0-100), counting ties as half.

    Mid-rank keeps a block of identical values (e.g. funding pinned at the interest
    rate) at the middle of that block instead of at its top or bottom.
    """
    if not window:
        return None
    ordered = sorted(window)
    below = bisect.bisect_left(ordered, value)
    equal = bisect.bisect_right(ordered, value) - below
    return 100.0 * (below + 0.5 * equal) / len(ordered)


@dataclass(frozen=True)
class FundingStats:
    ts: int
    rate_1h: float
    rate_8h: float
    premium: float  # 8h scale
    premium_pct: float | None
    funding_pct: float | None
    pinned_share: float | None  # share of the window pinned at the interest rate
    window_n: int


def funding_stats(
    latest_ts: int,
    latest_rate_1h: float,
    latest_premium: float,
    window_rates: Sequence[float],
    window_premiums: Sequence[float],
) -> FundingStats:
    pinned = sum(1 for r in window_rates if is_pinned(r))
    return FundingStats(
        ts=latest_ts,
        rate_1h=latest_rate_1h,
        rate_8h=to_8h(latest_rate_1h),
        premium=latest_premium,
        premium_pct=midrank_percentile(latest_premium, window_premiums),
        funding_pct=midrank_percentile(latest_rate_1h, window_rates),
        pinned_share=pinned / len(window_rates) if window_rates else None,
        window_n=len(window_rates),
    )
