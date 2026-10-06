from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

import httpx
import pytest

from hlflows import store
from hlflows.alerts import deliver, engine
from hlflows.alerts import rules as r
from hlflows.config import AlertsConfig, Config, StorageConfig
from hlflows.timeutil import HOUR_MS, MINUTE_MS

NOW = 1_790_000_000_000 - (1_790_000_000_000 % HOUR_MS)


def series(
    closes: list[float],
    deltas: list[float | None] | None = None,
    volume: float = 100.0,
    oi: list[float | None] | None = None,
    spot_deltas: list[float | None] | None = None,
    taker_cov: float = 1.0,
    open_last: bool = True,
) -> dict[str, Any]:
    """One timeframe's series. The last bar is the bar in progress unless open_last=False."""
    n = len(closes)
    deltas = deltas if deltas is not None else [0.0] * n
    cvd, total = [], 0.0
    for d in deltas:
        total += d or 0.0
        cvd.append(total)
    spot_cvd, st = [], 0.0
    for d in spot_deltas or [0.0] * n:
        st += d or 0.0
        spot_cvd.append(st)
    ts = [NOW - (n - i) * HOUR_MS for i in range(n)]
    return {
        "ts": [str(t) for t in ts], "ts_ms": ts,
        "closed": [True] * (n - 1) + [not open_last],
        "c": closes, "volume": [volume] * n, "delta": deltas, "cvd": cvd,
        "taker_cov": [taker_cov] * n, "spot_cvd": spot_cvd, "spot_volume": [volume] * n,
        "spot_taker_cov": [taker_cov] * n, "oi": oi or [None] * n,
        "watch_flow": [None] * n, "liq_long": [0.0] * n, "liq_short": [0.0] * n,
    }  # fmt: skip


def payload(coin: str = "HYPE", tfs: dict[str, Any] | None = None, **extra: Any) -> dict[str, Any]:
    c: dict[str, Any] = {"tfs": tfs or {}, "spot_significance": "normal", "summary": [],
                         "funding": None, "positioning": None, "liqmap": None}  # fmt: skip
    c.update(extra)
    return {"generated": "x", "generated_ms": NOW, "coins": {coin: c}}


CFG = AlertsConfig()


def test_cvd_divergence_bearish_and_bullish() -> None:
    up = series([100, 100.5, 101, 101.5, 102, 102.5, 103, 103.2], deltas=[-6.0] * 8)
    alerts = list(r.cvd_divergence(payload(tfs={"1h": up}), "HYPE", "1h", CFG))
    assert [(a.direction, a.tf) for a in alerts] == [("bearish", "1h")]
    assert alerts[0].facts["perp_taker_share"] == pytest.approx(-0.06)  # -36 / 600 volume
    assert alerts[0].facts["price_chg_pct"] == pytest.approx(3.0)
    down = series([103, 102.5, 102, 101.5, 101, 100.5, 100, 99], deltas=[5.0] * 8)
    assert [
        a.direction for a in r.cvd_divergence(payload(tfs={"1h": down}), "HYPE", "1h", CFG)
    ] == ["bullish"]


def test_cvd_divergence_needs_the_taker_split() -> None:
    up = series([100, 100.5, 101, 101.5, 102, 102.5, 103, 103.2], deltas=[-6.0] * 8,
                taker_cov=0.5)  # fmt: skip
    assert list(r.cvd_divergence(payload(tfs={"1h": up}), "HYPE", "1h", CFG)) == []


def test_no_divergence_when_takers_agree_or_move_is_small() -> None:
    agree = series([100, 100.5, 101, 101.5, 102, 102.5, 103, 103.2], deltas=[3.0] * 8)
    small = series([100, 100.1, 100.2, 100.3, 100.4, 100.5, 100.6, 100.6], deltas=[-3.0] * 8)
    for s in (agree, small):
        assert list(r.cvd_divergence(payload(tfs={"1h": s}), "HYPE", "1h", CFG)) == []


def test_only_closed_bars_count() -> None:
    # The big drop is in the bar still in progress: no alert yet.
    s = series([100, 100, 100, 100, 100, 100, 100, 90], deltas=[5.0] * 8)
    assert list(r.cvd_divergence(payload(tfs={"1h": s}), "HYPE", "1h", CFG)) == []


def test_spot_perp_divergence_only_for_primary_spot() -> None:
    s = series([100] * 8, deltas=[-6.0] * 8, spot_deltas=[6.0] * 8)
    assert [
        a.direction for a in r.spot_perp_divergence(payload(tfs={"4h": s}), "HYPE", "4h", CFG)
    ] == ["spot_buying"]
    thin = payload(tfs={"4h": s}, spot_significance="low")
    assert list(r.spot_perp_divergence(thin, "HYPE", "4h", CFG)) == []


def test_oi_change_labels() -> None:
    s = series([100, 100, 97, 97], deltas=[0, 0, -5.0, 0], oi=[1000, 1000, 1060, 1060])
    (alert,) = r.oi_change(payload(tfs={"1h": s}), "HYPE", "1h", CFG)
    assert alert.direction == "short_build" and alert.facts["oi_chg_pct"] == pytest.approx(6.0)
    s2 = series([100, 100, 103, 103], oi=[1000, 1000, 950, 950])
    assert [a.direction for a in r.oi_change(payload(tfs={"1h": s2}), "HYPE", "1h", CFG)] == [
        "covering"
    ]


def test_liq_cluster_near_mark() -> None:
    liqmap = {
        "mark": 100.0, "px": [96.0, 98.5, 101.5, 110.0], "long": [9e6, 2e6, 0, 0],
        "short": [0, 0, 6e6, 50e6], "n_long": [1, 1, 0, 0], "n_short": [0, 0, 1, 1],
    }  # fmt: skip
    alerts = list(r.liq_cluster(payload(liqmap=liqmap), "HYPE", CFG))
    # Longs within 3% below: 98.5 only (2M < 5M). Shorts within 3% above: 101.5 (6M >= 5M).
    assert [(a.direction, a.facts.get("short_ntl")) for a in alerts] == [("shorts_above", 6e6)]


def test_liquidations_and_watch_flow() -> None:
    s = series([100] * 4)
    s["liq_long"][2] = 1.5e6
    assert [a.direction for a in r.liquidations(payload(tfs={"1h": s}), "HYPE", CFG)] == ["longs"]
    w = series([100] * 4)
    w["watch_flow"] = [0.0, 0.0, 6e6, 6e6]
    assert [a.direction for a in r.watch_flow(payload(tfs={"4h": w}), "HYPE", "4h", CFG)] == [
        "net_long"
    ]
    assert list(r.watch_flow(payload(tfs={"1h": w}), "HYPE", "1h", CFG)) == []  # not configured


def test_premium_extreme() -> None:
    f = {"premium_pct": 97.0, "premium": 0.0007, "rate_8h": 0.0003, "ts": NOW, "at": "x"}
    assert [a.direction for a in r.premium_extreme(payload(funding=f), "HYPE", CFG)] == ["high"]
    f["premium_pct"] = 50.0
    assert list(r.premium_extreme(payload(funding=f), "HYPE", CFG)) == []


def test_disabled_rules_are_skipped() -> None:
    up = series([100, 100.5, 101, 101.5, 102, 102.5, 103, 103.2], deltas=[-6.0] * 8)
    p = payload(tfs={"1h": up})
    assert r.evaluate(p, AlertsConfig(timeframes=["1h"]))
    assert r.evaluate(p, AlertsConfig(timeframes=["1h"], disabled_rules=["cvd_divergence"])) == []


# --- engine ----------------------------------------------------------------------------


class FakeHttp:
    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self.client = httpx.Client(transport=httpx.MockTransport(self.handle))

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if "routines" in str(request.url):
            return httpx.Response(
                200, json={"claude_code_session_url": "https://claude.ai/code/s1"}
            )
        return httpx.Response(200, json={"id": "x"})


def setup_cfg(tmp_path: Path, p: dict[str, Any], **alerts: Any) -> Config:
    flows = tmp_path / "flows.json"
    flows.write_text(json.dumps(p))
    a = AlertsConfig(timeframes=["1h"], ntfy_topic="secret-topic", flows_path=str(flows),
                     dashboard_url="https://hl.example", **alerts)  # fmt: skip
    return Config(storage=StorageConfig(data_dir=str(tmp_path)), alerts=a)


def healthy(conn: sqlite3.Connection, now: int) -> None:
    with store.transaction(conn):
        conn.execute(
            "INSERT OR REPLACE INTO stream_state"
            " (stream, last_msg_ts, last_ok_ts, count, detail, updated_ts)"
            " VALUES ('recorder', ?, ?, 1, 'connected', ?)", (now, now, now)
        )  # fmt: skip


def bearish_payload() -> dict[str, Any]:
    up = series([100, 100.5, 101, 101.5, 102, 102.5, 103, 103.2], deltas=[-6.0] * 8)
    return payload(tfs={"1h": up})


def test_run_pushes_once_and_respects_cooldown(conn: sqlite3.Connection, tmp_path: Path) -> None:
    cfg = setup_cfg(tmp_path, bearish_payload())
    healthy(conn, NOW)
    http = FakeHttp()
    res = engine.run(cfg, conn, http.client, now=NOW)
    assert [a.rule for a in res.raised] == ["cvd_divergence"] and res.pushes == 1
    body = json.loads(http.requests[0].content)
    assert body["topic"] == "secret-topic" and body["priority"] == 3
    assert body["click"] == "https://hl.example/#coin=HYPE&tf=1h&view=charts"
    healthy(conn, NOW + 5 * MINUTE_MS)  # the recorder is still heartbeating
    again = engine.run(cfg, conn, http.client, now=NOW + 5 * MINUTE_MS)
    assert again.raised == [] and again.suppressed == 1
    row = conn.execute("SELECT pushed, facts FROM alerts").fetchone()
    assert row["pushed"] == 1 and json.loads(row["facts"])["bars"] == 6


def test_dry_run_records_nothing(conn: sqlite3.Connection, tmp_path: Path) -> None:
    cfg = setup_cfg(tmp_path, bearish_payload())
    healthy(conn, NOW)
    http = FakeHttp()
    res = engine.run(cfg, conn, http.client, dry_run=True, now=NOW)
    assert res.raised and http.requests == []
    assert conn.execute("SELECT COUNT(*) FROM alerts").fetchone()[0] == 0


def test_health_alerts(conn: sqlite3.Connection, tmp_path: Path) -> None:
    p = payload()
    p["generated_ms"] = NOW - 30 * MINUTE_MS
    cfg = setup_cfg(tmp_path, p)
    res = engine.run(cfg, conn, FakeHttp().client, dry_run=True, now=NOW)
    assert {a.direction for a in res.raised} == {"recorder_down", "dashboard_stale"}
    assert all(a.priority >= 4 for a in res.raised)


def test_many_alerts_become_one_summary(conn: sqlite3.Connection, tmp_path: Path) -> None:
    cfg = setup_cfg(tmp_path, bearish_payload(), max_pushes_per_run=0)
    healthy(conn, NOW)
    http = FakeHttp()
    engine.run(cfg, conn, http.client, now=NOW)
    (req,) = http.requests
    assert json.loads(req.content)["title"] == "1 flow alerts"


def test_routine_fire(conn: sqlite3.Connection, tmp_path: Path) -> None:
    cfg = setup_cfg(tmp_path, bearish_payload(),
                    routine_fire_url="https://api.anthropic.com/v1/claude_code/routines/trig_1/fire",
                    routine_token="tok")  # fmt: skip
    healthy(conn, NOW)
    http = FakeHttp()
    res = engine.run(cfg, conn, http.client, now=NOW)
    assert res.routine_url == "https://claude.ai/code/s1"
    fire = next(q for q in http.requests if "routines" in str(q.url))
    assert fire.headers["authorization"] == "Bearer tok"
    assert fire.headers["anthropic-beta"] == deliver.ROUTINE_BETA
    text = json.loads(json.loads(fire.content)["text"])
    assert text["alerts"][0]["rule"] == "cvd_divergence"
    assert "HYPE" in text["snapshot"] and text["snapshot"]["HYPE"]["recent_bars"]["1h"]["c"]
    # Within routine_min_interval_min: alerts still push, the routine isn't fired again.
    p2 = bearish_payload()
    p2["coins"]["HYPE"]["tfs"]["1h"]["ts_ms"][-2] += 1  # a new bar
    Path(cfg.alerts.flows_path).write_text(json.dumps(p2))
    cfg.alerts.cooldown_bars = 0
    cfg.alerts.min_cooldown_min = 0
    healthy(conn, NOW + 5 * MINUTE_MS)
    res2 = engine.run(cfg, conn, http.client, now=NOW + 5 * MINUTE_MS)
    assert res2.raised and res2.routine_url is None


def test_routine_text_stays_compact() -> None:
    p = payload(tfs={"1h": series([100.0] * 300)})
    alert = r.Alert("x", "HYPE", "1h", "d", None, 3, "t", "m", {})
    assert len(deliver.routine_text(p, [alert], CFG)) <= deliver.ROUTINE_TEXT_LIMIT
