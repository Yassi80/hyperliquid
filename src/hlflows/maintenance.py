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
from hlflows.timeutil import DAY_MS, floor_ms, from_ms

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


def archive_positions(
    conn: sqlite3.Connection, data_dir: str | Path, before: int
) -> list[tuple[int, int]]:
    """Move positions from whole UTC days before `before` into one Parquet file per day,
    then delete them from SQLite. Idempotent: re-running merges and deduplicates.
    Returns (day, rows in file) per day archived."""
    cutoff = floor_ms(before, DAY_MS)
    days = [
        int(r[0])
        for r in conn.execute(
            "SELECT DISTINCT (round_ts / ?) * ? FROM positions WHERE round_ts < ? ORDER BY 1",
            (DAY_MS, DAY_MS, cutoff),
        )
    ]
    done = []
    for day in days:
        rows = conn.execute(
            f"SELECT {', '.join(POSITION_COLUMNS)} FROM positions"
            " WHERE round_ts >= ? AND round_ts < ?",
            (day, day + DAY_MS),
        ).fetchall()
        df = pl.DataFrame([tuple(r) for r in rows], schema=POSITION_SCHEMA, orient="row")
        n = _write_merged(df, positions_day_path(data_dir, day), ["round_ts", "address", "coin"])
        with store.transaction(conn):
            conn.execute(
                "DELETE FROM positions WHERE round_ts >= ? AND round_ts < ?", (day, day + DAY_MS)
            )
        done.append((day, n))
        log.info("archived %s positions for %s", n, f"{from_ms(day):%Y-%m-%d}")
    return done


def read_archived_positions(
    data_dir: str | Path, round_ts: int, coin: str | None = None
) -> pl.DataFrame:
    path = positions_day_path(data_dir, floor_ms(round_ts, DAY_MS))
    if not path.exists():
        return pl.DataFrame(schema=POSITION_SCHEMA)
    df = pl.read_parquet(path).filter(pl.col("round_ts") == round_ts)
    return df.filter(pl.col("coin") == coin) if coin is not None else df


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
    """Consistent online copy of the database (safe while the recorder writes)."""
    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".tmp")
    tmp.unlink(missing_ok=True)
    target = sqlite3.connect(tmp)
    try:
        conn.backup(target, pages=4096)
    finally:
        target.close()
    shutil.move(tmp, dest)
    return dest
