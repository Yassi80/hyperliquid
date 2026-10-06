"""`hlflows alerts`: evaluate the rules on flows.json, apply cooldowns, store, push.

Runs right after each dashboard build (every 5 minutes on the VM). Health checks read
the database directly, so a stalled recorder or dashboard is reported even when the
flows data is stale.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path

import httpx

from hlflows import store
from hlflows.alerts import deliver
from hlflows.alerts.rules import Alert, Payload, evaluate
from hlflows.config import Config
from hlflows.timeutil import MINUTE_MS, TIMEFRAMES, now_ms

log = logging.getLogger(__name__)


def flows_path(cfg: Config) -> Path:
    return Path(cfg.alerts.flows_path or Path(cfg.storage.data_dir) / "flows.json")


def load_payload(path: Path) -> Payload | None:
    try:
        payload: Payload = json.loads(path.read_text())
        return payload
    except FileNotFoundError:
        return None


def _health(
    direction: str, priority: int, title: str, message: str, facts: dict[str, object]
) -> Alert:
    return Alert("health", "*", "-", direction, None, priority, title, message, dict(facts))


def health_alerts(
    cfg: Config, conn: sqlite3.Connection, payload: Payload | None, now: int
) -> list[Alert]:
    stale_ms = int(cfg.alerts.stale_after_min * MINUTE_MS)
    out: list[Alert] = []
    rec = conn.execute("SELECT * FROM stream_state WHERE stream = 'recorder'").fetchone()
    if rec is None or rec["detail"] == "stopped" or now - rec["updated_ts"] > 3 * MINUTE_MS:
        since = "never ran" if rec is None else (
            f"last heartbeat {(now - rec['updated_ts']) // MINUTE_MS} min ago")  # fmt: skip
        out.append(_health(
            "recorder_down", 5, "hlflows: recorder not running",
            f"The recorder isn't running ({since}). Trades, CVD and positions are not "
            "being recorded.", {"detail": None if rec is None else rec["detail"]},
        ))  # fmt: skip
    row = conn.execute("SELECT MAX(end_ts) FROM coverage WHERE stream = 'taker_1m'").fetchone()
    if row[0] is not None and now - row[0] > stale_ms:
        out.append(_health(
            "trades_stale", 5, "hlflows: no trade data",
            f"No trade data confirmed for {(now - row[0]) // MINUTE_MS} min.",
            {"covered_until": row[0]},
        ))  # fmt: skip
    generated = (payload or {}).get("generated_ms")
    if generated is None or now - int(generated) > stale_ms:
        age = "missing" if generated is None else f"{(now - int(generated)) // MINUTE_MS} min old"
        out.append(_health(
            "dashboard_stale", 4, "hlflows: dashboard data stale",
            f"The flows data is {age}: the dashboard build may be failing.",
            {"generated_ms": generated},
        ))  # fmt: skip
    return out


def cooldown_ms(cfg: Config, alert: Alert) -> int:
    """Health: an hour. Coin-level rules: coin_cooldown_min. Timeframe rules:
    cooldown_bars bars of that timeframe, at least min_cooldown_min."""
    a = cfg.alerts
    if alert.rule == "health":
        return 60 * MINUTE_MS
    tf = TIMEFRAMES.get(alert.tf)
    if tf is None:
        return int(a.coin_cooldown_min * MINUTE_MS)
    bar_ms = tf.fixed_ms if tf.fixed_ms is not None else 30 * 24 * 60 * MINUTE_MS  # ~1 month
    return int(max(a.min_cooldown_min * MINUTE_MS, a.cooldown_bars * bar_ms))


def is_new(conn: sqlite3.Connection, cfg: Config, alert: Alert, now: int) -> bool:
    """False if the same rule/coin/timeframe/direction fired for the same bar, or within
    its cooldown."""
    row = conn.execute(
        """SELECT ts, bar_ts FROM alerts WHERE rule = ? AND coin = ? AND tf = ? AND direction = ?
           ORDER BY ts DESC LIMIT 1""",
        (alert.rule, alert.coin, alert.tf, alert.direction),
    ).fetchone()
    if row is None:
        return True
    if alert.bar_ts is not None and row["bar_ts"] == alert.bar_ts:
        return False
    return bool(now - row["ts"] >= cooldown_ms(cfg, alert))


@dataclass
class RunResult:
    raised: list[Alert] = field(default_factory=list)
    suppressed: int = 0
    pushes: int = 0
    push_errors: list[str] = field(default_factory=list)
    routine_url: str | None = None
    routine_error: str | None = None


def run(
    cfg: Config,
    conn: sqlite3.Connection,
    http: httpx.Client,
    dry_run: bool = False,
    now: int | None = None,
) -> RunResult:
    now = now_ms() if now is None else now
    a = cfg.alerts
    result = RunResult()
    payload = load_payload(flows_path(cfg))
    candidates = [] if "health" in a.disabled_rules else health_alerts(cfg, conn, payload, now)
    if payload is not None:
        candidates += evaluate(payload, a)
    for alert in candidates:
        if is_new(conn, cfg, alert, now):
            result.raised.append(alert)
        else:
            result.suppressed += 1
    if dry_run or not result.raised:
        return result

    ids = []
    with store.transaction(conn):
        for alert in result.raised:
            cur = conn.execute(
                """INSERT INTO alerts (ts, rule, coin, tf, direction, bar_ts, priority, title,
                                       message, facts) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (now, alert.rule, alert.coin, alert.tf, alert.direction, alert.bar_ts,
                 alert.priority, alert.title, alert.message, json.dumps(alert.facts, default=str)),
            )  # fmt: skip
            ids.append(cur.lastrowid)

    if a.ntfy_topic:
        messages = (
            [deliver.ntfy_message(a, x) for x in result.raised]
            if len(result.raised) <= a.max_pushes_per_run
            else [deliver.summary_message(a, result.raised)]
        )
        for msg in messages:
            try:
                deliver.send_ntfy(http, a, msg)
                result.pushes += 1
            except httpx.HTTPError as exc:
                result.push_errors.append(repr(exc)[:200])
                log.error("ntfy push failed: %r", exc)
        with store.transaction(conn):
            error = result.push_errors[0] if result.push_errors else None
            conn.executemany(
                "UPDATE alerts SET pushed = ?, push_error = ? WHERE id = ?",
                [(int(error is None), error, i) for i in ids],
            )

    market = [x for x in result.raised if x.rule != "health"]
    if a.routine_fire_url and a.routine_token and market and payload is not None:
        last = conn.execute("SELECT MAX(routine_ts) FROM alerts").fetchone()[0]
        if last is None or now - last >= a.routine_min_interval_min * MINUTE_MS:
            try:
                result.routine_url = deliver.fire_routine(
                    http, a, deliver.routine_text(payload, market, a)
                )
                with store.transaction(conn):
                    conn.executemany(
                        "UPDATE alerts SET routine_ts = ? WHERE id = ?", [(now, i) for i in ids]
                    )
            except httpx.HTTPError as exc:
                result.routine_error = repr(exc)[:200]
                log.error("routine fire failed: %r", exc)
    return result
