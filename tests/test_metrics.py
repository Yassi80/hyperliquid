from __future__ import annotations

import pytest

from hlflows.metrics import cvd_slope, pct_change


def test_cvd_slope_constant_buying() -> None:
    # Delta +10 every bar on volume 100: CVD rises 10/bar = 0.1 average bars' volume.
    assert cvd_slope([10.0] * 5, [100.0] * 5) == pytest.approx(0.1)


def test_cvd_slope_sign_and_missing() -> None:
    s = cvd_slope([-5.0, -5.0, -5.0, -5.0], [50.0] * 4)
    assert s is not None and s == pytest.approx(-0.1)
    assert cvd_slope([1.0, None, 1.0], [1.0, 1.0, 1.0]) is None
    assert cvd_slope([1.0, 1.0], [1.0, 1.0]) is None
    assert cvd_slope([0.0, 0.0, 0.0], [0.0, 0.0, 0.0]) is None


def test_pct_change() -> None:
    assert pct_change(100.0, 105.0) == pytest.approx(5.0)
    assert pct_change(None, 1.0) is None
    assert pct_change(0.0, 1.0) is None
