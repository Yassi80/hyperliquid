"""`status`: recorder liveness, per-stream freshness, open gaps, weight headroom, disk."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from rich.console import Group, RenderableType
from rich.table import Table
from rich.text import Text

from hlflows.budget import WeightBudget
from hlflows.config import Config
from hlflows.timeutil import fmt_ms, now_ms


def _age(ts: int | None, now: int) -> str:
    if ts is None:
        return "—"
    s = (now - ts) / 1000
    if s < 0:  # e.g. funding covered through the hour in progress
        return "current"
    if s < 120:
        return f"{s:.0f}s ago"
    if s < 7200:
        return f"{s / 60:.0f}m ago"
    if s < 172800:
        return f"{s / 3600:.1f}h ago"
    return f"{s / 86400:.1f}d ago"


def _dir_size(path: Path) -> int:
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file()) if path.exists() else 0


def _mb(n: int) -> str:
    return f"{n / 1e6:,.1f} MB"


def render(cfg: Config, conn: sqlite3.Connection) -> RenderableType:
    now = now_ms()
    parts: list[RenderableType] = []

    rows = {r["stream"]: r for r in conn.execute("SELECT * FROM stream_state ORDER BY stream")}
    rec = rows.get("recorder")
    alive = rec is not None and now - rec["updated_ts"] < 3 * cfg.recorder.heartbeat_s * 1000
    if rec is None:
        parts.append(Text("Recorder: never run against this database.", style="yellow"))
    elif rec["detail"] == "stopped":
        parts.append(
            Text(f"Recorder: stopped cleanly {_age(rec['updated_ts'], now)}", style="yellow")
        )
    elif alive:
        parts.append(
            Text(f"Recorder: running (pid {rec['count']}, {rec['detail']}), "
                 f"heartbeat {_age(rec['updated_ts'], now)}", style="green")
        )  # fmt: skip
    else:
        parts.append(
            Text(f"Recorder: NOT running (last heartbeat {_age(rec['updated_ts'], now)})",
                 style="bold red")
        )  # fmt: skip

    streams = Table(title="Streams", title_justify="left")
    for col in ("stream", "last data", "last ok", "count", "detail"):
        streams.add_column(col, justify="right" if col == "count" else "left")
    for name, r in rows.items():
        if name == "recorder":
            continue
        count = f"{r['count']:,}"
        if name == "ws":
            count += " reconnects"
        streams.add_row(
            name, _age(r["last_msg_ts"], now), _age(r["last_ok_ts"], now), count, r["detail"] or ""
        )
    if streams.row_count:
        parts.append(streams)

    gaps = conn.execute(
        """SELECT stream, market, start_ts, end_ts, reason, recoverable FROM gaps
           WHERE status = 'open' ORDER BY start_ts DESC LIMIT 20"""
    ).fetchall()
    n_open = conn.execute("SELECT COUNT(*) FROM gaps WHERE status = 'open'").fetchone()[0]
    g = Table(title=f"Open gaps ({n_open}, newest 20)", title_justify="left")
    for col in ("stream", "market", "from", "to", "recoverable", "reason"):
        g.add_column(col)
    for r in gaps:
        g.add_row(r["stream"], r["market"], fmt_ms(r["start_ts"]), fmt_ms(r["end_ts"]),
                  r["recoverable"], r["reason"])  # fmt: skip
    parts.append(g)

    streams_order = ["bars_1m", "taker_1m", "ctx_1m", "bars_1h", "bars_1d", "funding", "positions"]
    ends: dict[str, dict[str, int]] = {}
    for r in conn.execute(
        "SELECT stream, market, MAX(end_ts) AS end_ts FROM coverage GROUP BY stream, market"
    ):
        ends.setdefault(r["market"], {})[r["stream"]] = r["end_ts"]
    cov = Table(title="Covered until (age)", title_justify="left")
    cov.add_column("market")
    for name in streams_order:
        cov.add_column(name)
    for market in sorted(ends):
        cov.add_row(market, *(_age(ends[market].get(n), now) for n in streams_order))
    parts.append(cov)

    counts = {
        (r["status"], r["source"]): r["n"]
        for r in conn.execute("SELECT status, source, COUNT(*) AS n FROM wallets GROUP BY 1, 2")
    }
    tagged = conn.execute(
        "SELECT COUNT(*) FROM wallets WHERE tags LIKE '%mm%' OR tags LIKE '%vault%'"
        " OR role = 'vault'"
    ).fetchone()[0]
    seen = conn.execute("SELECT COUNT(*) FROM addresses_seen").fetchone()[0]
    due = conn.execute(
        "SELECT COUNT(*) FROM addresses_seen WHERE probe_after_ts <= ?", (now,)
    ).fetchone()[0]
    last_round = conn.execute(
        "SELECT * FROM snapshot_rounds ORDER BY round_ts DESC LIMIT 1"
    ).fetchone()
    lines = [
        f"Watchlist: {counts.get(('active', 'manual'), 0)} manual + "
        f"{counts.get(('active', 'trades'), 0)} discovered active "
        f"(max {cfg.wallets.max_active}), "
        f"{sum(v for (st, _), v in counts.items() if st == 'dormant')} dormant, "
        f"{tagged} tagged vault/mm",
        f"Addresses seen in trades: {seen:,} ({due:,} due for a probe)",
    ]
    if last_round is not None:
        lines.append(
            f"Last polling round: {fmt_ms(last_round['round_ts'])} "
            f"({_age(last_round['round_ts'], now)}), {last_round['wallets_polled']} wallets, "
            f"{last_round['wallets_failed']} failed, {last_round['duration_ms'] / 1000:.1f}s"
        )
    parts.append(Text("\n".join(lines)))

    total, budget_lines = cfg.weight_budget_lines()
    parts.append(
        Text(
            f"Configured REST budget: {total:.0f}/min of ceiling {cfg.weight_ceiling}\n"
            + "\n".join(budget_lines),
            style="dim",
        )
    )

    used = WeightBudget(conn, cfg.weight_ceiling).used()
    staged = conn.execute("SELECT COUNT(*) FROM trades_staging").fetchone()[0]
    db_path = Path(cfg.storage.db_path)
    db_size = sum(p.stat().st_size for p in (db_path, Path(f"{db_path}-wal")) if p.exists())
    trades_size = _dir_size(Path(cfg.storage.data_dir) / "trades")
    parts.append(
        Text(
            f"REST weight last 60s: {used} / ceiling {cfg.weight_ceiling} "
            f"(IP limit {cfg.rate_limit.ip_weight_per_min})\n"
            f"Staged trades: {staged:,}\n"
            f"Disk: database {_mb(db_size)}, raw trades {_mb(trades_size)}"
        )
    )
    return Group(*parts)
