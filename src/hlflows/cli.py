"""hlflows command line."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.logging import RichHandler
from rich.table import Table

from hlflows import db
from hlflows import export as export_mod
from hlflows import show as show_mod
from hlflows import status as status_mod
from hlflows.api.client import InfoClient
from hlflows.backfill import run_backfill
from hlflows.budget import WeightBudget
from hlflows.config import Config, load_config
from hlflows.timeutil import fmt_ms, now_ms, parse_date, parse_timeframes

app = typer.Typer(
    help="Hyperliquid flows data: backfill, record, inspect, export.", no_args_is_help=True
)
console = Console()


class State:
    config_path: str | None = None


def _config() -> Config:
    return load_config(State.config_path)


@app.callback()
def main(
    config: Annotated[
        str | None, typer.Option("--config", "-c", help="Path to hlflows.toml")
    ] = None,
    verbose: Annotated[bool, typer.Option("--verbose", "-v")] = False,
) -> None:
    State.config_path = config
    logging.basicConfig(
        level=logging.INFO if verbose else logging.WARNING,
        format="%(message)s",
        handlers=[RichHandler(console=Console(stderr=True), show_path=False)],
    )


def _coins(cfg: Config, coin: str | None) -> list[str]:
    if coin is None:
        return list(cfg.markets.coins)
    coins = [c.strip().upper() for c in coin.split(",")]
    unknown = [c for c in coins if c not in cfg.markets.coins]
    if unknown:
        raise typer.BadParameter(f"not in markets.coins: {', '.join(unknown)}")
    return coins


@app.command()
def backfill(
    from_: Annotated[str, typer.Option("--from", help="Start date (UTC), e.g. 2026-01-01")],
    coin: Annotated[str | None, typer.Option(help="Coin(s), comma-separated; default all")] = None,
    to: Annotated[str | None, typer.Option(help="End date (UTC); default now")] = None,
    source: Annotated[str, typer.Option(help="api (s3 is deferred)")] = "api",
) -> None:
    """Pull all history the REST API allows and report what couldn't be fetched."""
    if source != "api":
        raise typer.BadParameter("only --source api is implemented; S3 backfill is deferred")
    cfg = _config()
    coins = _coins(cfg, coin)
    start = parse_date(from_)
    end = parse_date(to) if to else now_ms()

    async def go() -> None:
        conn = db.connect(cfg.storage.db_path)
        budget = WeightBudget(db.connect(cfg.storage.db_path), cfg.weight_ceiling)
        async with InfoClient(
            budget, cfg.api.info_url, cfg.api.timeout_s, cfg.api.max_retries
        ) as client:
            report = await run_backfill(cfg, conn, client, coins, start, end)
        table = Table(title=f"Backfill {fmt_ms(start)} → {fmt_ms(end)} UTC", title_justify="left")
        for col in ("market", "stream", "rows", "from", "to", "notes"):
            table.add_column(col, justify="right" if col == "rows" else "left")
        for r in report.results:
            table.add_row(
                r.market,
                r.stream,
                f"{r.rows:,}",
                fmt_ms(r.first_ts) if r.first_ts else "—",
                fmt_ms(r.end_ts) if r.end_ts else "—",
                "\n".join(r.notes),
            )
        console.print(table)
        for w in report.warnings:
            console.print(f"[yellow]warning:[/] {w}")

    asyncio.run(go())


@app.command()
def show(
    coin: Annotated[str, typer.Option(help="Coin, e.g. ETH")],
    tf: Annotated[str, typer.Option(help="Timeframes, comma-separated")] = "1h,4h,1d",
) -> None:
    """Per-timeframe summary: price, OI, delta, CVD slope, funding."""
    cfg = _config()
    (c,) = _coins(cfg, coin)
    conn = db.connect(cfg.storage.db_path)
    console.print(show_mod.render(cfg, conn, c, parse_timeframes(tf)))
    console.print(f"[dim]{show_mod.CVD_SLOPE_HELP}[/]")


@app.command()
def export(
    coin: Annotated[str, typer.Option(help="Coin, e.g. ETH")],
    tf: Annotated[str, typer.Option(help="Timeframe")],
    from_: Annotated[str, typer.Option("--from", help="Start date (UTC)")],
    to: Annotated[str | None, typer.Option(help="End date (UTC); default now")] = None,
    table: Annotated[
        str,
        typer.Option(help="bars | spot | ctx | funding | positioning | liquidations | liqmap"),
    ] = "bars",
    fmt: Annotated[str, typer.Option("--format", help="csv | parquet")] = "csv",
    out: Annotated[Path | None, typer.Option(help="Output file")] = None,
) -> None:
    """Export resampled data to CSV or Parquet."""
    cfg = _config()
    (c,) = _coins(cfg, coin)
    (timeframe,) = parse_timeframes(tf)
    start = parse_date(from_)
    end = parse_date(to) if to else now_ms()
    conn = db.connect(cfg.storage.db_path)
    df = export_mod.build(conn, cfg, table, c, timeframe, start, end)
    if out is None:
        out = (
            Path(cfg.storage.data_dir)
            / "exports"
            / f"{c}_{table}_{timeframe.name}_{from_}_{to or 'now'}.{fmt}"
        )
    export_mod.write(df, out, fmt)
    console.print(f"wrote {df.height:,} rows to {out}")


@app.command()
def record(
    duration: Annotated[
        float | None, typer.Option(help="Stop cleanly after this many seconds (for testing)")
    ] = None,
) -> None:
    """Run the recorder: trades and asset context over WebSocket, REST polling on schedules.

    Stops cleanly on SIGINT/SIGTERM. Safe to restart at any time; gaps are detected and
    logged on resume.
    """
    import signal

    from hlflows.recorder.core import Recorder

    cfg = _config()

    async def go() -> None:
        conn = db.connect(cfg.storage.db_path)
        budget = WeightBudget(db.connect(cfg.storage.db_path), cfg.weight_ceiling)
        async with InfoClient(
            budget, cfg.api.info_url, cfg.api.timeout_s, cfg.api.max_retries
        ) as client:
            recorder = Recorder(cfg, conn, client)
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, recorder.request_stop)
            await recorder.run(duration)

    logging.getLogger("hlflows").setLevel(logging.INFO)
    asyncio.run(go())


@app.command()
def status() -> None:
    """Recorder health: streams, last updates, open gaps, rate-limit headroom, disk."""
    cfg = _config()
    conn = db.connect(cfg.storage.db_path)
    console.print(status_mod.render(cfg, conn))


@app.command()
def liqmap(
    coin: Annotated[str, typer.Option(help="Coin, e.g. ETH")],
    bucket_pct: Annotated[float | None, typer.Option(help="Bucket width, % of mark")] = None,
    range_pct: Annotated[float | None, typer.Option(help="Range around mark, %")] = None,
    at: Annotated[
        str | None, typer.Option(help="Use the last snapshot before this UTC time")
    ] = None,
) -> None:
    """Liquidation map from watchlist positions, with long/short coverage of open interest."""
    from hlflows import liqmap as liqmap_mod

    cfg = _config()
    (c,) = _coins(cfg, coin)
    conn = db.connect(cfg.storage.db_path)
    bucket = bucket_pct or cfg.wallets.liqmap_bucket_pct
    rng = range_pct or cfg.wallets.liqmap_range_pct
    at_ms = parse_date(at) if at else None
    console.print(liqmap_mod.render(cfg, conn, c, bucket, rng, at_ms))


wallets_app = typer.Typer(help="Manage the wallet watchlist.", no_args_is_help=True)
app.add_typer(wallets_app, name="wallets")


def _address(text: str) -> str:
    a = text.strip().lower()
    if len(a) != 42 or not a.startswith("0x") or any(ch not in "0123456789abcdef" for ch in a[2:]):
        raise typer.BadParameter(f"not an address: {text}")
    return a


@wallets_app.command("add")
def wallets_add(
    address: str,
    label: Annotated[str | None, typer.Option(help="A name to show in lists")] = None,
) -> None:
    """Watch an address (as a manual wallet: always polled, never pruned)."""
    from hlflows import store
    from hlflows.wallets import store as ws

    cfg = _config()
    a = _address(address)
    conn = db.connect(cfg.storage.db_path)
    with store.transaction(conn):
        ws.set_wallet(conn, a, "active", "manual (cli)", now_ms(), source="manual", label=label)
    console.print(f"watching {a}; the recorder polls it from its next round")
    if a not in cfg.wallets.manual:
        console.print("[dim]Tip: also add it to wallets.manual in the config to keep it there.[/]")


@wallets_app.command("remove")
def wallets_remove(address: str) -> None:
    """Stop watching an address."""
    from hlflows import store
    from hlflows.wallets import store as ws

    cfg = _config()
    a = _address(address)
    conn = db.connect(cfg.storage.db_path)
    if ws.get_wallet(conn, a) is None:
        raise typer.BadParameter(f"{a} is not in the watchlist")
    with store.transaction(conn):
        ws.set_wallet(conn, a, "removed", "cli", now_ms())
    console.print(f"removed {a}")
    if a in cfg.wallets.manual:
        console.print("[yellow]It is in wallets.manual; the recorder re-adds it on start.[/]")


@wallets_app.command("list")
def wallets_list(
    all_: Annotated[bool, typer.Option("--all", help="Include dormant and removed")] = False,
) -> None:
    """The watchlist, with each wallet's latest positions in the configured coins."""
    from hlflows.liqmap import usd
    from hlflows.wallets import positioning as pos

    cfg = _config()
    conn = db.connect(cfg.storage.db_path)
    where = "" if all_ else "WHERE status = 'active'"
    wallets = conn.execute(
        f"SELECT * FROM wallets {where} ORDER BY status, COALESCE(qualifying_ntl, 0) DESC"
    ).fetchall()
    round_ts = pos.latest_round(conn)
    held: dict[str, list[str]] = {}
    if round_ts is not None:
        for r in conn.execute(
            "SELECT address, coin, szi, position_value FROM positions WHERE round_ts = ?",
            (round_ts,),
        ):
            sign = "+" if r["szi"] > 0 else "-"
            held.setdefault(r["address"], []).append(
                f"{r['coin']} {sign}{usd(r['position_value'])}"
            )
    table = Table(title=f"Watchlist ({len(wallets)})", title_justify="left")
    for col in ("address", "label", "source", "status", "tags", "role", "account", "positions"):
        table.add_column(col, justify="right" if col == "account" else "left")
    for w in wallets:
        table.add_row(
            w["address"], w["label"] or "", w["source"], w["status"], w["tags"] or "",
            w["role"] or "?", usd(w["account_value"]), ", ".join(held.get(w["address"], [])),
        )  # fmt: skip
    console.print(table)


@wallets_app.command("discover")
def wallets_discover(
    limit: Annotated[int, typer.Option(help="How many due addresses to probe")] = 20,
    dry_run: Annotated[bool, typer.Option("--dry-run", help="Probe but change nothing")] = False,
) -> None:
    """Probe due addresses from the trades feed now (the recorder does this continuously)."""
    from hlflows.liqmap import usd
    from hlflows.wallets.tracker import WalletTracker

    cfg = _config()

    async def go() -> None:
        conn = db.connect(cfg.storage.db_path)
        budget = WeightBudget(db.connect(cfg.storage.db_path), cfg.weight_ceiling)
        async with InfoClient(budget, cfg.api.info_url, cfg.api.timeout_s,
                              cfg.api.max_retries) as client:  # fmt: skip
            results = await WalletTracker(cfg, conn, client).probe_due(limit, dry_run)
        if not results:
            console.print("Nothing due. Addresses come from the recorder's trades feed.")
            return
        table = Table(title="Probe results" + (" (dry run)" if dry_run else ""),
                      title_justify="left")  # fmt: skip
        for col in ("address", "notional", "qualifies", "outcome"):
            table.add_column(col)
        for r in results:
            table.add_row(r.address, usd(r.ntl), "yes" if r.qualifies else "no", r.outcome)
        console.print(table)

    asyncio.run(go())


@app.command()
def maintain(
    prune: Annotated[
        bool, typer.Option(help="Also delete local Parquet older than local_retention_days")
    ] = False,
) -> None:
    """Archive positions older than storage.positions_hot_days to Parquet, keeping their
    hour-close rounds in SQLite for storage.positions_hourly_days (and optionally prune
    local Parquet already synced elsewhere). Safe while the recorder runs."""
    from datetime import UTC, datetime

    from hlflows import maintenance
    from hlflows.timeutil import DAY_MS

    cfg = _config()
    conn = db.connect(cfg.storage.db_path)
    now = now_ms()
    archived = maintenance.archive_positions(
        conn,
        cfg.storage.data_dir,
        before=now - cfg.storage.positions_hot_days * DAY_MS,
        hourly_before=now - cfg.storage.positions_hourly_days * DAY_MS,
    )
    console.print(f"archived positions for {len(archived)} day(s)")
    if prune:
        removed = maintenance.prune_local(
            cfg.storage.data_dir, cfg.storage.local_retention_days, datetime.now(UTC)
        )
        console.print(f"pruned {len(removed)} local Parquet file(s)")


@app.command()
def backup(dest: Annotated[Path, typer.Argument(help="Destination .sqlite file")]) -> None:
    """Consistent online copy of the database (safe while the recorder writes)."""
    from hlflows import maintenance

    cfg = _config()
    conn = db.connect(cfg.storage.db_path)
    path = maintenance.backup(conn, dest)
    console.print(f"backed up to {path} ({path.stat().st_size / 1e6:,.1f} MB)")


@app.command()
def dashboard(
    coin: Annotated[str | None, typer.Option(help="Coin(s), comma-separated; default all")] = None,
    tf: Annotated[str, typer.Option(help="Timeframes to include")] = "15m,1h,4h,1d",
    bars: Annotated[int, typer.Option(help="Bars per timeframe")] = 200,
    out: Annotated[Path | None, typer.Option(help="Output .html file")] = None,
    json_out: Annotated[
        Path | None, typer.Option("--json", help="Also write the data as JSON (for alerts)")
    ] = None,
    open_: Annotated[bool, typer.Option("--open", help="Open it in the browser")] = False,
) -> None:
    """Write a self-contained HTML dashboard of the flows data (charts, tables, liq map)."""
    import webbrowser

    from hlflows import dashboard as dash

    cfg = _config()
    coins = _coins(cfg, coin)
    conn = db.connect(cfg.storage.db_path)
    payload = dash.build(cfg, conn, coins, parse_timeframes(tf), bars)
    path = out or Path(cfg.storage.data_dir) / "dashboard.html"
    path.parent.mkdir(parents=True, exist_ok=True)
    targets = [(path, dash.render_html(payload))]
    if json_out is not None:
        targets.append((json_out, dash.render_json(payload)))
    for target, text in targets:
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_suffix(target.suffix + ".tmp")
        tmp.write_text(text)
        tmp.chmod(0o644)
        tmp.replace(target)  # atomic: readers never see a half-written file
        console.print(f"wrote {target}")
    if open_:
        webbrowser.open(path.resolve().as_uri())


@app.command()
def alerts(
    dry_run: Annotated[
        bool, typer.Option("--dry-run", help="Evaluate, print, send nothing")
    ] = False,
    test_push: Annotated[
        bool, typer.Option("--test-push", help="Send a test notification")
    ] = False,
    test_routine: Annotated[
        bool, typer.Option("--test-routine", help="Fire the Claude routine with a test payload")
    ] = False,
    history: Annotated[int, typer.Option(help="Show the last N alerts instead")] = 0,
) -> None:
    """Check the alert rules on flows.json (written by `dashboard --json`), push new alerts
    via ntfy and, if configured, hand them to the Claude Code routine."""
    import json as json_mod

    import httpx

    from hlflows.alerts import deliver, engine
    from hlflows.alerts.rules import Alert

    cfg = _config()
    a = cfg.alerts
    conn = db.connect(cfg.storage.db_path)

    if history:
        rows = conn.execute("SELECT * FROM alerts ORDER BY ts DESC LIMIT ?", (history,)).fetchall()
        table = Table(title=f"Last {len(rows)} alerts", title_justify="left")
        for col in ("time (UTC)", "rule", "coin", "tf", "direction", "title", "pushed", "routine"):
            table.add_column(col)
        for r in rows:
            pushed = "yes" if r["pushed"] else (r["push_error"] or "no")[:30]
            routine = fmt_ms(r["routine_ts"]) if r["routine_ts"] else ""
            table.add_row(fmt_ms(r["ts"]), r["rule"], r["coin"], r["tf"], r["direction"],
                          r["title"], pushed, routine)  # fmt: skip
        console.print(table)
        return

    with httpx.Client() as http:
        if test_push or test_routine:
            sample = Alert(
                "test", "BTC", "4h", "test", None, 3, "hlflows test alert",
                "If you can read this, alerts reach your phone.", {"test": True},
            )  # fmt: skip
            if test_push:
                if not a.ntfy_topic:
                    raise typer.BadParameter("set alerts.ntfy_topic in the config first")
                deliver.send_ntfy(http, a, deliver.ntfy_message(a, sample))
                console.print(f"sent a test push to topic {a.ntfy_topic[:6]}…")
            if test_routine:
                if not (a.routine_fire_url and a.routine_token):
                    raise typer.BadParameter("set alerts.routine_fire_url and routine_token first")
                payload = engine.load_payload(engine.flows_path(cfg)) or {"coins": {}}
                url = deliver.fire_routine(http, a, deliver.routine_text(payload, [sample], a))
                console.print(f"routine started: {url}")
            return

        if not a.enabled:
            console.print("alerts are disabled (alerts.enabled = false)")
            return
        result = engine.run(cfg, conn, http, dry_run=dry_run)

    for x in result.raised:
        console.print(f"[bold]{x.title}[/]  ({x.rule}, p{x.priority})\n  {x.message}")
        if dry_run:
            console.print(f"  [dim]{json_mod.dumps(x.facts, default=str)}[/]")
    console.print(
        f"{len(result.raised)} new, {result.suppressed} suppressed by cooldown"
        + ("" if dry_run else f"; {result.pushes} pushed")
        + (f"; routine: {result.routine_url}" if result.routine_url else "")
    )
    for err in result.push_errors + ([result.routine_error] if result.routine_error else []):
        console.print(f"[red]error:[/] {err}")
    if result.push_errors or result.routine_error:
        raise typer.Exit(1)
