from __future__ import annotations

import pytest

from hlflows.api import types as t
from hlflows.funding import (
    INTEREST_1H,
    funding_1h_from_premium,
    funding_stats,
    is_pinned,
    midrank_percentile,
    to_8h,
    venue_rate_to_8h,
)
from tests.conftest import fixture_bytes


def test_formula_reproduces_recorded_funding_history() -> None:
    """Every recorded hourly rate follows from its 8h-scale premium via the documented formula."""
    records = t.decode(fixture_bytes("fundingHistory_HYPE.json"), list[t.FundingRecord])
    assert len(records) > 100
    for r in records:
        assert funding_1h_from_premium(r.premium) == pytest.approx(r.fundingRate, abs=1e-9)


def test_pinned_band() -> None:
    # Funding pins at the interest rate for -0.04% <= P <= +0.06% (8h scale).
    for p in (-0.0004, 0.0, 0.0001, 0.0006):
        assert is_pinned(funding_1h_from_premium(p))
    assert not is_pinned(funding_1h_from_premium(-0.00041))
    assert not is_pinned(funding_1h_from_premium(0.00061))
    assert funding_1h_from_premium(0.001) == pytest.approx((0.001 - 0.0005) / 8)


def test_hourly_cap() -> None:
    assert funding_1h_from_premium(10.0) == 0.04
    assert funding_1h_from_premium(-10.0) == -0.04


def test_eight_hour_equivalents() -> None:
    assert to_8h(INTEREST_1H) == pytest.approx(0.0001)
    assert venue_rate_to_8h(0.0000125, 1) == pytest.approx(0.0001)
    assert venue_rate_to_8h(0.0001, 8) == pytest.approx(0.0001)
    assert venue_rate_to_8h(0.0001, 4) == pytest.approx(0.0002)


def test_midrank_percentile_ties() -> None:
    window = [1.0, 2.0, 2.0, 2.0, 3.0]
    assert midrank_percentile(2.0, window) == pytest.approx(50.0)
    assert midrank_percentile(1.0, window) == pytest.approx(10.0)
    assert midrank_percentile(3.0, window) == pytest.approx(90.0)
    assert midrank_percentile(4.0, window) == pytest.approx(100.0)
    assert midrank_percentile(0.0, window) == pytest.approx(0.0)
    assert midrank_percentile(1.0, []) is None


def test_funding_stats_reports_pinned_share() -> None:
    rates = [INTEREST_1H] * 8 + [0.00002, -0.00001]
    premiums = [0.0001 * i for i in range(10)]
    s = funding_stats(0, INTEREST_1H, premiums[-1], rates, premiums)
    assert s.pinned_share == pytest.approx(0.8)
    assert s.rate_8h == pytest.approx(0.0001)
    assert s.premium_pct == pytest.approx(95.0)
    assert s.funding_pct == pytest.approx(50.0)  # middle of the pinned block
    assert s.window_n == 10
