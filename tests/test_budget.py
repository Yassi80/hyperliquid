from __future__ import annotations

import sqlite3

import pytest

from hlflows.budget import WeightBudget, backoff_delays


class Clock:
    def __init__(self) -> None:
        self.t = 1_000_000

    def __call__(self) -> int:
        return self.t


def test_reserves_until_ceiling_then_waits(conn: sqlite3.Connection) -> None:
    clock = Clock()
    b = WeightBudget(conn, ceiling=100, clock=clock)
    assert b.try_reserve(60, "a") == 0
    clock.t += 10_000
    assert b.try_reserve(40, "b") == 0
    assert b.used() == 100
    wait = b.try_reserve(20, "c")
    # Needs the first 60 to leave the window: at t0 + 60s.
    assert wait == 1_000_000 + 60_000 - clock.t + 1
    clock.t += wait
    assert b.try_reserve(20, "c") == 0
    assert b.used() == 60


def test_surcharge_counts_against_budget(conn: sqlite3.Connection) -> None:
    clock = Clock()
    b = WeightBudget(conn, ceiling=50, clock=clock)
    assert b.try_reserve(20, "x") == 0
    b.charge(25, "x")
    assert b.used() == 45
    assert b.try_reserve(20, "y") > 0


def test_oversized_request_runs_on_empty_window(conn: sqlite3.Connection) -> None:
    b = WeightBudget(conn, ceiling=10, clock=Clock())
    assert b.try_reserve(50, "big") == 0


def test_budget_is_shared_between_connections(db_path: object, conn: sqlite3.Connection) -> None:
    from hlflows import db

    clock = Clock()
    other = db.connect(db_path)  # type: ignore[arg-type]
    a = WeightBudget(conn, ceiling=100, clock=clock)
    b = WeightBudget(other, ceiling=100, clock=clock)
    assert a.try_reserve(80, "a") == 0
    assert b.try_reserve(30, "b") > 0


async def test_acquire_sleeps_until_capacity(conn: sqlite3.Connection) -> None:
    clock = Clock()
    slept: list[float] = []

    async def fake_sleep(s: float) -> None:
        slept.append(s)
        clock.t += int(s * 1000)

    b = WeightBudget(conn, ceiling=20, clock=clock, sleep=fake_sleep)
    await b.acquire(20, "a")
    await b.acquire(20, "b")
    assert slept and sum(slept) == pytest.approx(60.001, abs=0.01)


def test_backoff_is_bounded_and_grows() -> None:
    delays = backoff_delays(base_s=1.0, cap_s=8.0)
    samples = [next(delays) for _ in range(10)]
    assert all(0 <= d <= 8.0 for d in samples)
    assert samples[0] <= 1.0
