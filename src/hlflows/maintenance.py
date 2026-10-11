"""Housekeeping for unattended runs: archive old positions to Parquet, prune local files
that have been synced elsewhere, and take consistent online backups of the database."""

from __future__ import annotations

import logging
import shutil
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from hlflows import store
from hlflows.timeutil import DAY_MS, HOUR_MS, floor_ms, from_ms, now_ms, to_ms

log = logging.getLogger(__name__)

POSITION_COLUMNS = (
    "round_ts", "address", "coin", "szi", "entry_px", "position_value", "lev_type", "lev",
    "eff_lev", "liq_px", "upnl", "margin_used", "account_value",
)  # fmt: skip
POSITION_SCHEMA = {
    "round_ts": pl.Int64, "address": pl.Utf8, "coin": pl.Utf8, "szi": pl.Float64,
    "entry_px": pl.Float64, "position_value": pl.Float64, "lev_type": pl.Utf8,
    "lev": pl.Float64, "eff_lev": pl.Float64, "liq_px": pl.Float64, "upnl": pl.Float64,
    "margin_used": pl.Float64, "account_value": pl.Float64,
}  # fmt: skip


def positions_dir(data_dir: str | Path) -> Path:
    return Path(data_dir) / "positions"


def positions_day_path(data_dir: str | Path, day_ms: int) -> Path:
    return positions_dir(data_dir) / f"date={from_ms(day_ms):%Y-%m-%d}.parquet"


def _write_merged(df: pl.DataFrame, path: Path, key: list[str]) -> int:
    if path.exists():
        df = pl.concat([pl.read_parquet(path), df])
    df = df.unique(subset=key, keep="last").sort(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".parquet.tmp")
    df.write_parquet(tmp, compression="zstd")
    tmp.replace(path)
    return df.height


def hour_close_rounds(conn: sqlite3.Connection, start: int, end: int) -> dict[int, int]:
    """{hour start: last polling round in that hour} for rounds in [start, end). Bars of
    1h and longer close on the hour, and charts take the last round before each close."""
    return {
        int(r[0]) * HOUR_MS: int(r[1])
        for r in conn.execute(
            "SELECT round_ts / ?, MAX(round_ts) FROM snapshot_rounds"
            " WHERE round_ts >= ? AND round_ts < ? GROUP BY 1",
            (HOUR_MS, start, end),
        )
    }


def archive_positions(
    conn: sqlite3.Connection, data_dir: str | Path, before: int, hourly_before: int | None = None
) -> list[tuple[int, int]]:
    """Archive positions of whole UTC days before `before`. Each day is written once, in
    full, to data/positions/date=*.parquet (see the archived_days table); SQLite then keeps
    only its hour-close rounds, which 1h-and-longer charts read, until `hourly_before`
    (None: keep none). Works an hour at a time, so memory stays small and each delete
    holds the write lock only briefly (the recorder writes every few seconds).
    Returns (day, rows in file) for each day written."""
    cutoff = floor_ms(before, DAY_MS)
    hourly_cutoff = (
        cutoff if hourly_before is None else min(cutoff, floor_ms(hourly_before, DAY_MS))
    )
    archived = {int(r[0]) for r in conn.execute("SELECT day FROM archived_days")}
    _restore_hour_close_rounds(conn, data_dir, hourly_cutoff, cutoff, archived)
    days = [
        int(r[0])
        for r in conn.execute(
            "SELECT DISTINCT (round_ts / ?) * ? FROM positions WHERE round_ts < ? ORDER BY 1",
            (DAY_MS, DAY_MS, cutoff),
        )
    ]
    done = []
    for day in days:
        if day not in archived:
            n = _write_day(conn, data_dir, day)
            with store.transaction(conn):
                conn.execute(
                    "INSERT OR REPLACE INTO archived_days VALUES (?, ?, ?)", (day, n, now_ms())
                )
            done.append((day, n))
            log.info("archived %s positions for %s", n, f"{from_ms(day):%Y-%m-%d}")
        keep = {} if day < hourly_cutoff else hour_close_rounds(conn, day, day + DAY_MS)
        _thin_day(conn, day, keep)
    return done


def _write_day(conn: sqlite3.Connection, data_dir: str | Path, day: int) -> int:
    hours = range(day, day + DAY_MS, HOUR_MS)
    frames = [
        pl.DataFrame(
            [tuple(r) for r in conn.execute(
                f"SELECT {', '.join(POSITION_COLUMNS)} FROM positions"
                " WHERE round_ts >= ? AND round_ts < ?",
                (h, h + HOUR_MS),
            )],
            schema=POSITION_SCHEMA,
            orient="row",
        )
        for h in hours
    ]  # fmt: skip
    path = positions_day_path(data_dir, day)
    return _write_merged(pl.concat(frames), path, ["round_ts", "address", "coin"])


def _thin_day(conn: sqlite3.Connection, day: int, keep: dict[int, int]) -> None:
    """Delete the day's positions except the rounds in `keep` ({hour start: round})."""
    stored = {
        int(r[0])
        for r in conn.execute(
            "SELECT DISTINCT round_ts FROM positions WHERE round_ts >= ? AND round_ts < ?",
            (day, day + DAY_MS),
        )
    }
    if stored <= set(keep.values()):
        return
    for h in range(day, day + DAY_MS, HOUR_MS):
        with store.transaction(conn):
            conn.execute(
                "DELETE FROM positions WHERE round_ts >= ? AND round_ts < ? AND round_ts != ?",
                (h, h + HOUR_MS, keep.get(h, -1)),
            )


def _restore_hour_close_rounds(
    conn: sqlite3.Connection, data_dir: str | Path, start: int, end: int, archived: set[int]
) -> None:
    """Days in [start, end) archived in full before hour-close rounds were kept in SQLite:
    copy those rounds back from the day's Parquet file and record the day as archived."""
    root = positions_dir(data_dir)
    if not root.exists():
        return
    for path in sorted(root.glob("date=*.parquet")):
        d = _partition_date(path)
        if d is None:
            continue
        day = to_ms(d)
        if not start <= day < end or day in archived:
            continue
        has_rows = conn.execute(
            "SELECT 1 FROM positions WHERE round_ts >= ? AND round_ts < ? LIMIT 1",
            (day, day + DAY_MS),
        ).fetchone()
        if has_rows:
            continue  # not archived yet: the main loop handles it
        keep = list(hour_close_rounds(conn, day, day + DAY_MS).values())
        lf = pl.scan_parquet(path)
        df = lf.filter(pl.col("round_ts").is_in(keep)).select(POSITION_COLUMNS).collect()
        n = int(lf.select(pl.len()).collect().item())
        with store.transaction(conn):
            conn.executemany(
                f"INSERT OR IGNORE INTO positions ({', '.join(POSITION_COLUMNS)})"
                f" VALUES ({', '.join('?' * len(POSITION_COLUMNS))})",
                df.iter_rows(),
            )
            conn.execute(
                "INSERT OR REPLACE INTO archived_days VALUES (?, ?, ?)", (day, n, now_ms())
            )
        archived.add(day)
        log.info("restored %s hour-close positions for %s", df.height, f"{d:%Y-%m-%d}")


def read_archived_positions(
    data_dir: str | Path, round_ts: int, coin: str | None = None
) -> pl.DataFrame:
    path = positions_day_path(data_dir, floor_ms(round_ts, DAY_MS))
    if not path.exists():
        return pl.DataFrame(schema=POSITION_SCHEMA)
    df = pl.read_parquet(path).filter(pl.col("round_ts") == round_ts)
    return df.filter(pl.col("coin") == coin) if coin is not None else df


def read_archived_rounds(
    data_dir: str | Path, day_ms: int, rounds: list[int], coin: str, columns: list[str]
) -> pl.DataFrame:
    """Archived positions of one coin for several rounds of the same day, reading only
    the needed columns and rows of that day's file, once."""
    path = positions_day_path(data_dir, day_ms)
    if not path.exists():
        return pl.DataFrame(schema={c: POSITION_SCHEMA[c] for c in columns})
    return (
        pl.scan_parquet(path)
        .filter(pl.col("coin") == coin, pl.col("round_ts").is_in(rounds))
        .select(columns)
        .collect()
    )


def _partition_date(path: Path) -> datetime | None:
    for part in (path.stem, *(p.name for p in path.parents)):
        if part.startswith("date="):
            try:
                return datetime.strptime(part[5:], "%Y-%m-%d").replace(tzinfo=UTC)
            except ValueError:
                return None
    return None


def prune_local(data_dir: str | Path, older_than_days: int, now: datetime) -> list[Path]:
    """Delete local raw-trade and archived-position Parquet files whose date partition is
    older than `older_than_days`. Only for files already copied elsewhere (e.g. GCS)."""
    if older_than_days <= 0:
        return []
    cutoff = now.timestamp() - older_than_days * 86400
    removed = []
    for sub in ("trades", "positions"):
        root = Path(data_dir) / sub
        if not root.exists():
            continue
        for path in sorted(root.rglob("*.parquet")):
            d = _partition_date(path)
            if d is not None and d.timestamp() + 86400 <= cutoff:
                path.unlink()
                removed.append(path)
        for directory in sorted((p for p in root.rglob("*") if p.is_dir()), reverse=True):
            if not any(directory.iterdir()):
                directory.rmdir()
    return removed


def backup(conn: sqlite3.Connection, dest: str | Path) -> Path:
    """Consistent online copy of the database (safe while the recorder writes).

    VACUUM INTO copies one read snapshot in a single pass. The step-wise backup API
    restarts from scratch whenever another connection writes between steps, so with the
    recorder writing every few seconds it never finished on a database larger than RAM."""
    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".tmp")
    tmp.unlink(missing_ok=True)
    conn.execute("VACUUM INTO ?", (str(tmp),))
    shutil.move(tmp, dest)
    return dest
