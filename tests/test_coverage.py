from __future__ import annotations

import sqlite3

from hlflows import coverage as cov


def test_merge_and_subtract() -> None:
    assert cov.merge_intervals([(5, 10), (0, 3), (3, 4), (9, 12), (20, 20)]) == [(0, 4), (5, 12)]
    assert cov.subtract((0, 20), [(2, 5), (8, 10)]) == [(0, 2), (5, 8), (10, 20)]
    assert cov.subtract((3, 4), [(0, 10)]) == []
    assert cov.contains([(0, 5), (5, 10)], (0, 10))
    assert not cov.contains([(0, 5), (6, 10)], (0, 10))


def test_add_coverage_merges_rows(conn: sqlite3.Connection) -> None:
    cov.add_coverage(conn, "s", "BTC", 0, 10)
    cov.add_coverage(conn, "s", "BTC", 20, 30)
    cov.add_coverage(conn, "s", "BTC", 10, 20)  # bridges both
    cov.add_coverage(conn, "s", "BTC", 5, 15)  # already covered
    cov.add_coverage(conn, "s", "ETH", 0, 5)
    assert cov.get_coverage(conn, "s", "BTC") == [(0, 30)]
    assert cov.get_coverage(conn, "s", "ETH") == [(0, 5)]
    assert cov.get_coverage(conn, "s", "BTC", 40, 50) == []


def test_gap_detection_and_fill(conn: sqlite3.Connection) -> None:
    cov.add_coverage(conn, "funding", "BTC", 0, 10)
    cov.add_coverage(conn, "funding", "BTC", 15, 30)
    holes = cov.subtract((0, 30), cov.get_coverage(conn, "funding", "BTC"))
    assert holes == [(10, 15)]
    cov.record_gap(conn, "funding", "BTC", 10, 15, "hour missing", "rest")
    cov.record_gap(conn, "funding", "BTC", 10, 15, "hour missing", "rest")  # idempotent
    assert conn.execute("SELECT COUNT(*) FROM gaps").fetchone()[0] == 1

    assert cov.close_filled_gaps(conn, "funding", "BTC") == 0
    cov.add_coverage(conn, "funding", "BTC", 10, 15)
    assert cov.close_filled_gaps(conn, "funding", "BTC") == 1
    assert conn.execute("SELECT status FROM gaps").fetchone()[0] == "filled"
