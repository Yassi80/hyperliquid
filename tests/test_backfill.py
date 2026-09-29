from __future__ import annotations

import sqlite3

import httpx

from hlflows import coverage as cov
from hlflows.api.client import InfoClient
from hlflows.backfill import run_backfill
from hlflows.budget import WeightBudget
from hlflows.config import Config, MarketsConfig
from hlflows.timeutil import DAY_MS, now_ms
from tests.conftest import fixture_json
from tests.fake_api import URL, FakeInfo


def _count(conn: sqlite3.Connection, sql: str) -> int:
    return int(conn.execute(sql).fetchone()[0])


async def _run(conn: sqlite3.Connection, fake: FakeInfo, start: int) -> None:
    cfg = Config(markets=MarketsConfig(coins=["HYPE"], spot_tokens={"HYPE": "HYPE"}))
    http = httpx.AsyncClient(transport=httpx.MockTransport(fake))
    async with InfoClient(WeightBudget(conn, 1000), URL, http=http) as client:
        report = await run_backfill(cfg, conn, client, ["HYPE"], start)
    assert report.warnings == []


async def test_backfill_writes_and_is_idempotent(conn: sqlite3.Connection) -> None:
    start = now_ms() - 10 * DAY_MS
    fake = FakeInfo()
    await _run(conn, fake, start)

    markets = {r[0]: r[1] for r in conn.execute("SELECT market, hl_name FROM markets")}
    assert markets == {"HYPE": "HYPE", "HYPE/spot": "@107"}

    n_1h = _count(conn, "SELECT COUNT(*) FROM bars WHERE market = 'HYPE' AND res = '1h'")
    closed_1h = [c for c in fixture_json("candleSnapshot_ETH_1h.json") if c["T"] < now_ms()]
    assert n_1h == len(closed_1h)
    assert cov.get_coverage(conn, "bars_1h", "HYPE")

    n_funding = _count(conn, "SELECT COUNT(*) FROM funding WHERE coin = 'HYPE'")
    in_range = [r for r in fixture_json("fundingHistory_HYPE.json") if r["time"] >= start]
    assert n_funding == len(in_range)
    assert _count(conn, "SELECT COUNT(*) FROM asset_ctx WHERE coin = 'HYPE'") == 1
    assert _count(conn, "SELECT COUNT(*) FROM predicted_funding WHERE coin = 'HYPE'") >= 1

    # 1d candles: none returned inside the depth window means no trades: covered, no gap.
    assert conn.execute("SELECT COUNT(*) FROM gaps WHERE stream = 'bars_1d'").fetchone()[0] == 0
    assert cov.get_coverage(conn, "bars_1d", "HYPE")
    # 1m candles: requested 10 days back, beyond the ~3.5 day depth -> recoverable only via S3.
    gap_1m = conn.execute(
        "SELECT reason, recoverable FROM gaps WHERE stream = 'bars_1m' AND market = 'HYPE'"
    ).fetchone()
    assert gap_1m["recoverable"] == "s3" and "depth" in gap_1m["reason"]

    snapshot = {
        t: [tuple(r) for r in conn.execute(f"SELECT * FROM {t} ORDER BY 1, 2, 3")]
        for t in ("bars", "funding", "coverage", "markets")
    }
    await _run(conn, FakeInfo(), start)
    for t, rows in snapshot.items():
        again = [tuple(r) for r in conn.execute(f"SELECT * FROM {t} ORDER BY 1, 2, 3")]
        if t == "markets":  # updated_ts changes; the rest must match
            assert [r[:5] for r in again] == [r[:5] for r in rows]
        else:
            assert again == rows, t


async def test_backfill_detects_missing_funding_hours(conn: sqlite3.Connection) -> None:
    records = fixture_json("fundingHistory_HYPE.json")
    dropped = records[100:103]
    fake = FakeInfo()

    def funding(body: dict[str, object]) -> httpx.Response:
        keep = [r for r in records if r not in dropped and r["time"] >= body["startTime"]]
        return httpx.Response(200, json=keep[:500])

    fake.overrides["fundingHistory"] = funding
    await _run(conn, fake, records[0]["time"])
    gaps = conn.execute(
        "SELECT start_ts, end_ts, recoverable, status FROM gaps WHERE stream = 'funding'"
    ).fetchall()
    assert len(gaps) == 1
    assert gaps[0]["end_ts"] - gaps[0]["start_ts"] == 3 * 3_600_000
    assert gaps[0]["recoverable"] == "rest" and gaps[0]["status"] == "open"
