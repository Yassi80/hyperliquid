"""Derived flow metrics."""

from __future__ import annotations

from collections.abc import Sequence
from itertools import accumulate


def cvd_slope(deltas: Sequence[float | None], volumes: Sequence[float | None]) -> float | None:
    """Normalized CVD slope over a window of bars.

    Least-squares slope of cumulative delta against bar index, divided by the mean bar
    volume (taker buy + sell) in the window, so the result is comparable across coins:
    +1 means CVD rose on average by one average bar's volume per bar; the range in
    practice is -1..+1. Returns None with fewer than 3 bars or any missing delta.
    """
    if len(deltas) < 3 or len(deltas) != len(volumes):
        return None
    if any(d is None for d in deltas) or any(v is None for v in volumes):
        return None
    ds = [float(d) for d in deltas if d is not None]
    vs = [float(v) for v in volumes if v is not None]
    mean_vol = sum(vs) / len(vs)
    if mean_vol <= 0:
        return None
    cvd = list(accumulate(ds))
    n = len(cvd)
    x_mean = (n - 1) / 2
    y_mean = sum(cvd) / n
    sxx = sum((i - x_mean) ** 2 for i in range(n))
    sxy = sum((i - x_mean) * (y - y_mean) for i, y in enumerate(cvd))
    return (sxy / sxx) / mean_vol


def pct_change(old: float | None, new: float | None) -> float | None:
    if old is None or new is None or old == 0:
        return None
    return (new - old) / old * 100
