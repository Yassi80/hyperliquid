"""Delivery: ntfy push notifications and the optional Claude Code routine trigger."""

from __future__ import annotations

import json
from typing import Any

import httpx

from hlflows.alerts.rules import Alert, Payload
from hlflows.config import AlertsConfig

ROUTINE_BETA = "experimental-cc-routine-2026-04-01"
ROUTINE_TEXT_LIMIT = 16_000  # keep the fire payload compact
TAGS = {5: ["rotating_light"], 4: ["warning"], 3: ["chart_with_upwards_trend"]}


def view_url(cfg: AlertsConfig, alert: Alert) -> str | None:
    if not cfg.dashboard_url or alert.coin == "*":
        return None
    tf = alert.tf if alert.tf not in ("-", "") else "4h"
    return f"{cfg.dashboard_url.rstrip('/')}/#coin={alert.coin}&tf={tf}&view=charts"


def ntfy_message(cfg: AlertsConfig, alert: Alert) -> dict[str, Any]:
    """ntfy's JSON publish format (avoids header encoding issues with non-ASCII titles)."""
    msg: dict[str, Any] = {
        "topic": cfg.ntfy_topic,
        "title": alert.title,
        "message": alert.message,
        "priority": alert.priority,
        "tags": TAGS.get(alert.priority, []),
    }
    if url := view_url(cfg, alert):
        msg["click"] = url
    return msg


def summary_message(cfg: AlertsConfig, alerts: list[Alert]) -> dict[str, Any]:
    lines = [f"• {a.title}" for a in sorted(alerts, key=lambda a: -a.priority)]
    first = max(alerts, key=lambda a: a.priority)
    msg = ntfy_message(cfg, first)
    msg["title"] = f"{len(alerts)} flow alerts"
    msg["message"] = "\n".join(lines)
    return msg


def send_ntfy(client: httpx.Client, cfg: AlertsConfig, message: dict[str, Any]) -> None:
    headers = {"Authorization": f"Bearer {cfg.ntfy_token}"} if cfg.ntfy_token else {}
    resp = client.post(cfg.ntfy_server.rstrip("/"), json=message, headers=headers, timeout=15)
    resp.raise_for_status()


def compact_snapshot(payload: Payload, coin: str, bars: int = 12) -> dict[str, Any]:
    """The essentials for one coin: per-timeframe summary, funding, positioning, the
    nearest liquidation clusters and the last few closed bars of each timeframe."""
    c = payload["coins"][coin]
    keep = ("tf", "bar", "close", "price_chg_pct", "oi_chg_pct", "delta", "taker_cov",
            "cvd_slope", "spot_delta", "funding_8h", "premium", "watch_dnet_usd",
            "liq_long", "liq_short")  # fmt: skip
    out: dict[str, Any] = {
        "summary": [{k: r.get(k) for k in keep} for r in c.get("summary", [])],
        "funding": c.get("funding"),
        "positioning": c.get("positioning"),
        "spot_significance": c.get("spot_significance"),
    }
    m = c.get("liqmap")
    if m:
        mark = m["mark"]
        near = [
            {"px": round(px, 6), "long": lo, "short": sh}
            for px, lo, sh in zip(m["px"], m["long"], m["short"], strict=True)
            if (lo or sh) and abs(px / mark - 1) <= 0.1
        ]
        out["liqmap_near"] = {"mark": mark, "buckets_within_10pct": near}
    series = {}
    for tf, s in (c.get("tfs") or {}).items():
        idx = [i for i, closed in enumerate(s.get("closed") or []) if closed][-bars:]
        series[tf] = {k: [s[k][i] for i in idx] for k in
                      ("ts", "c", "oi", "delta", "cvd", "spot_cvd", "watch_flow",
                       "liq_long", "liq_short") if s.get(k)}  # fmt: skip
    out["recent_bars"] = series
    return out


def routine_text(payload: Payload, alerts: list[Alert], cfg: AlertsConfig) -> str:
    coins = sorted({a.coin for a in alerts if a.coin != "*"})
    body: dict[str, Any] = {
        "source": "hlflows",
        "generated_utc": payload.get("generated"),
        "dashboard": cfg.dashboard_url or None,
        "alerts": [
            {
                "rule": a.rule,
                "coin": a.coin,
                "tf": a.tf,
                "direction": a.direction,
                "title": a.title,
                "message": a.message,
                "facts": a.facts,
            }
            for a in alerts
        ],
        "snapshot": {coin: compact_snapshot(payload, coin) for coin in coins},
    }
    text = json.dumps(body, separators=(",", ":"), default=str)
    bars = 12
    while len(text) > ROUTINE_TEXT_LIMIT and bars > 2:
        bars //= 2
        body["snapshot"] = {coin: compact_snapshot(payload, coin, bars) for coin in coins}
        text = json.dumps(body, separators=(",", ":"), default=str)
    return text[:ROUTINE_TEXT_LIMIT]


def fire_routine(client: httpx.Client, cfg: AlertsConfig, text: str) -> str:
    """Start the Claude Code routine (API trigger). Returns the session URL."""
    resp = client.post(
        cfg.routine_fire_url,
        headers={
            "Authorization": f"Bearer {cfg.routine_token}",
            "anthropic-beta": ROUTINE_BETA,
            "anthropic-version": "2023-06-01",
        },
        json={"text": text},
        timeout=30,
    )
    resp.raise_for_status()
    return str(resp.json().get("claude_code_session_url", ""))
