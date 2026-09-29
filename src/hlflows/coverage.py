"""Coverage (where data is authoritative) and gap bookkeeping.

Ranges are half-open [start, end) in UTC ms. Inside a covered range, a missing bar
means no trades happened; outside, it means we don't know.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable

from hlflows.timeutil import now_ms

Interval = tuple[int, int]


def merge_intervals(intervals: Iterable[Interval]) -> list[Interval]:
    """Union of intervals; touching intervals are joined."""
    out: list[Interval] = []
    for start, end in sorted(i for i in intervals if i[1] > i[0]):
        if out and start <= out[-1][1]:
            out[-1] = (out[-1][0], max(out[-1][1], end))
        else:
            out.append((start, end))
    return out


def subtract(span: Interval, covered: Iterable[Interval]) -> list[Interval]:
    """Parts of `span` not covered by `covered`."""
    start, end = span
    holes: list[Interval] = []
    cursor = start
    for c_start, c_end in merge_intervals(covered):
        if c_end <= cursor:
            continue
        if c_start >= end:
            break
        if c_start > cursor:
            holes.append((cursor, c_start))
        cursor = max(cursor, c_end)
        if cursor >= end:
            break
    if cursor < end:
        holes.append((cursor, end))
    return holes


def contains(covered: Iterable[Interval], span: Interval) -> bool:
    return not subtract(span, covered)


def add_coverage(conn: sqlite3.Connection, stream: str, market: str, start: int, end: int) -> None:
    if end <= start:
        return
    rows = conn.execute(
        """SELECT start_ts, end_ts FROM coverage
           WHERE stream = ? AND market = ? AND start_ts <= ? AND end_ts >= ?""",
        (stream, market, end, start),
    ).fetchall()
    merged = merge_intervals([(start, end), *((r[0], r[1]) for r in rows)])
    assert len(merged) == 1
    conn.executemany(
        "DELETE FROM coverage WHERE stream = ? AND market = ? AND start_ts = ?",
        [(stream, market, r[0]) for r in rows],
    )
    conn.execute(
        "INSERT INTO coverage (stream, market, start_ts, end_ts) VALUES (?, ?, ?, ?)",
        (stream, market, *merged[0]),
    )


def get_coverage(
    conn: sqlite3.Connection,
    stream: str,
    market: str,
    start: int | None = None,
    end: int | None = None,
) -> list[Interval]:
    sql = "SELECT start_ts, end_ts FROM coverage WHERE stream = ? AND market = ?"
    args: list[object] = [stream, market]
    if end is not None:
        sql += " AND start_ts < ?"
        args.append(end)
    if start is not None:
        sql += " AND end_ts > ?"
        args.append(start)
    return [(r[0], r[1]) for r in conn.execute(sql + " ORDER BY start_ts", args)]


def record_gap(
    conn: sqlite3.Connection,
    stream: str,
    market: str,
    start: int,
    end: int,
    reason: str,
    recoverable: str,
) -> None:
    if end <= start:
        return
    conn.execute(
        """INSERT INTO gaps (stream, market, start_ts, end_ts, reason, recoverable, status,
                             detected_ts)
           VALUES (?, ?, ?, ?, ?, ?, 'open', ?)
           ON CONFLICT (stream, market, start_ts) DO UPDATE SET
             end_ts = excluded.end_ts, reason = excluded.reason,
             recoverable = excluded.recoverable""",
        (stream, market, start, end, reason, recoverable, now_ms()),
    )


def close_filled_gaps(conn: sqlite3.Connection, stream: str, market: str) -> int:
    """Mark open gaps that are now fully covered as filled. Returns how many changed."""
    changed = 0
    for row in conn.execute(
        "SELECT start_ts, end_ts FROM gaps WHERE stream = ? AND market = ? AND status = 'open'",
        (stream, market),
    ).fetchall():
        span = (row[0], row[1])
        if contains(get_coverage(conn, stream, market, *span), span):
            conn.execute(
                "UPDATE gaps SET status = 'filled'"
                " WHERE stream = ? AND market = ? AND start_ts = ?",
                (stream, market, row[0]),
            )
            changed += 1
    return changed
