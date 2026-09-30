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
    """Archive positions older than storage.positions_hot_days to Parquet (and optionally
    prune local Parquet already synced elsewhere). Safe while the recorder runs."""
    from datetime import UTC, datetime

    from hlflows import maintenance
    from hlflows.timeutil import DAY_MS

    cfg = _config()
    conn = db.connect(cfg.storage.db_path)
    before = now_ms() - cfg.storage.positions_hot_days * DAY_MS
    archived = maintenance.archive_positions(conn, cfg.storage.data_dir, before)
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
    path.write_text(dash.render_html(payload))
    console.print(f"wrote {path}")
    if open_:
        webbrowser.open(path.resolve().as_uri())
