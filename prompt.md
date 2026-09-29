# Build a CLI tool that extracts Hyperliquid flows data

## Context

I trade crypto on higher timeframes using a flows framework: monthly/weekly for regime, daily for positioning state, 4h for location, 1h as the trigger timeframe, and 15m for execution only. I read open interest, futures CVD, spot CVD, funding and liquidations, and classify each timeframe into a flow state (healthy trend, crowded, distribution, absorption, covering, unwind, short build, reset).

For the major centralized exchanges I use Coinglass. This tool covers Hyperliquid only, for two reasons: Coinglass coverage of Hyperliquid is thinner, and Hyperliquid exposes something no centralized exchange does, which is per-wallet positions with leverage and liquidation prices. That lets me see actual positioning and real liquidation levels instead of modelled heatmaps. That part is the main point of the tool.

The data will also feed later research (backtesting which flow patterns precede which outcomes), so fidelity and honest data-quality labelling matter more than speed.

Public data only. No private keys, no order placement.

## Before writing code

1. Use Python (3.12+). I'm more comfortable in TypeScript, but I'll later add a research layer (backtesting, flow-state calibration) to the same codebase, and Python's analytics ecosystem wins there. Choose the rest of the stack and justify it in a few sentences: mature async WebSocket and HTTP libraries, whether the official Python SDK adds anything for public data, a good local analytics/storage story, ease of running unattended, and strict typing of API payloads so API drift fails loudly. Expose the stored data through a query layer that returns DataFrames, so the CLI and future research code share it.
2. Verify every endpoint, field name, rate limit and history window below against the current official Hyperliquid API docs. My notes may be outdated. Where the docs contradict me, follow the docs and tell me what changed.
3. Present a short plan (architecture, storage schema, command list, phases) and wait for my confirmation before implementing.

## Data sources (verify each)

**Perp market context** — `POST https://api.hyperliquid.xyz/info`
- `metaAndAssetCtxs`: funding, open interest, mark and oracle price, premium, impact prices, day volume. Snapshot only, so poll and store on a schedule; there is no public OI history. Check whether the `activeAssetCtx` WebSocket subscription is a better way to capture the same fields continuously.
- `fundingHistory`: realized funding per coin, for backfill.
- `predictedFundings`: predicted next funding for Hyperliquid and other venues. Store the other venues too; it gives me a free cross-venue funding comparison.
- `candleSnapshot`: OHLCV for backfill. Note its extra rate-limit weight per returned items, and that it only returns the most recent ~5000 candles per interval (about 3.5 days at 1m), so deep history must come from coarser intervals or S3.

**CVD**
- Candles have no taker split (confirm), so delta must come from the public `trades` WebSocket channel. Each trade's `side` field encodes the aggressor (confirm the exact semantics of `A`/`B` from the docs and a live sample). Aggregate into per-bar buy volume, sell volume and delta.
- Store raw trades too (partitioned files are fine), not only aggregated bars. Bars lose information permanently; raw trades allow re-aggregation, large-trade analysis and re-running wallet discovery later.
- On subscribe, the trades channel may replay a snapshot of recent trades. Deduplicate by trade id (`tid`).
- Check whether the trades payload includes the addresses of both sides (a `users` field). If it does, it's useful for liquidation inference and wallet discovery below.
- Spot: discover spot markets via `spotMeta` (names are non-obvious, e.g. `@<index>`). BTC, ETH and SOL spot on Hyperliquid are wrapped tokens (UBTC, UETH, USOL) and thin: record their spot CVD but label it `low_significance`; my real spot read for those comes from Coinglass. HYPE is different: its primary spot market is on Hyperliquid, so HYPE spot CVD is first-class and must not be labelled low-significance.
- CVD gaps cannot be refilled over REST (no public trade-history endpoint, confirm). Only the S3 archive can fill them, with a lag. Gap tracking must distinguish recoverable gaps from ones recoverable only via S3 or not at all.

**Historical backfill**
- The docs point to an S3 bucket for large batch requests. Investigate what it contains (asset contexts, trades or fills history, L2 snapshots), whether it's requester-pays and roughly what that costs, and whether it allows backfilling OI and CVD history. Report before implementing.

**Wallet positioning** (the core feature)
- Poll `clearinghouseState` over REST for a watchlist of wallets: per-coin position size and side, entry price, leverage, margin mode, liquidation price, unrealized PnL.
- Do not use user-specific WebSocket subscriptions for this. They're capped at around 10 unique users per IP.
- Wallet discovery: official sources only, no unofficial leaderboard. Capture every address seen in the public trades feed (both sides of each trade), probe each with `clearinghouseState` within its own weight sub-budget (addresses from large trades first; per-coin large-trade thresholds configurable, propose defaults), and promote wallets holding a qualifying position in the configured coins into the watchlist. Tag and exclude market makers (high fill count, small net position). Keep a manual list in config that's always watched and never pruned. Document the cold-start limitation: wallets holding old positions without trading stay invisible until they trade.
- Watchlist size and poll interval are configurable. Propose defaults that fit the REST weight budget with headroom (I'd guess a few hundred wallets polled every 1–5 minutes, but size it from the verified limits).
- Large vaults (HLP and other big vaults) are market-making/backstop counterparties, not directional traders. Tag known vaults and exclude them from positioning aggregates by default (configurable), while still storing their snapshots.
- Derived outputs from the watchlist, per coin: net long vs short notional, leverage distribution, and a liquidation map (notional by liquidation-price bucket, split long/short).
- Coverage metric: open interest counts one side (total longs = total shorts), so report long coverage (watchlist long notional ÷ OI notional) and short coverage (watchlist short notional ÷ OI notional) separately, plus combined coverage as (long + short) ÷ (2 × OI). Never divide both sides by OI once. Show coverage on every positioning output, because a liquidation map covering 15% of OI means something different from one covering 60%.
- Cross-margin liquidation prices depend on the whole account and move with price and other positions. The liquidation map is a point-in-time snapshot; say so in output and in the caveats.

**Liquidations**
- There is no native exchange-wide liquidation feed. Don't fake one.
- Investigate whether liquidations can be inferred from public data. Two known routes, each with limits:
  - Watched wallets: `userFills` (REST) carries a `liquidation` object on fills where the user was liquidated, including the method (market vs backstop; confirm). This is reliable but covers only the watchlist.
  - Trades feed: Hyperliquid first sends liquidations to the book as market orders; only if the book can't absorb them does the liquidator vault take the position (backstop). Trades with the liquidator vault as counterparty therefore capture backstop liquidations only, a small subset. Label them `partial` and name them backstop-only, never as total liquidations.
- Implement only what's reliable, and label inferred liquidations as `partial`.
- Optional, off by default: an adapter interface for a paid provider with a global liquidation stream (e.g. GoldRush or Bitquery), enabled only if I add an API key to config.

## Funding normalization

Hyperliquid funding is paid hourly and includes a fixed interest component (0.01% per 8h, i.e. 0.00125% per hour). Verify the exact formula; my understanding is roughly `funding = premium + clamp(interest − premium, −0.05%, +0.05%)` (per 8h, divided by 8 per hour, with an hourly cap). That clamp means that whenever the premium is near the interest rate, funding pins at exactly the interest rate, so the premium cannot be recovered by subtracting interest from funding.

Store:
- the raw hourly funding rate,
- an 8h-equivalent rate (hourly × 8, to compare with centralized exchanges),
- the premium as reported directly in the asset context (`premium` field from `metaAndAssetCtxs` / `activeAssetCtx`), not derived from funding.

Hyperliquid runs structurally richer than centralized venues, so compute percentiles against each coin's own trailing history (configurable window, e.g. 30 days). Because funding sits exactly at the interest rate much of the time, a funding percentile will have a large block of ties. Compute the percentile primarily on the premium, and if you also show a funding percentile, use a defined tie rule (e.g. mid-rank) and show the share of the window pinned at the interest rate.

## Pairs

Config file with a list of coins. Starting set: BTC, ETH, SOL, HYPE. Use the canonical perp dex by default; ignore HIP-3 builder dexes unless configured.

## Storage

- Local, file-based, analytics-friendly (e.g. DuckDB, or SQLite plus Parquet export; justify your choice).
- Store per-bar delta, not cumulative CVD, so cumulative series can be anchored at any start date.
- OI in coin units. Derive USD at query time.
- All timestamps UTC. Weekly bars open Monday 00:00 UTC by default, configurable.
- Base resolution for recorded bars: 1m or 5m (your recommendation), resampled at query time to 15m, 1h, 4h, 1d, 1w, 1M.
- Positioning snapshots stored as time series, so I can see how the watchlist's positioning and liquidation map evolve.
- Idempotent upserts. Explicit gap tracking per metric and time range. A data-quality flag per row where relevant (`inferred`, `partial`, `low_significance`).

## Commands (adjust names as you see fit)

- `backfill --coin ETH --from 2026-01-01` — pull all history the API and S3 allow, report what couldn't be fetched and why.
- `record` — long-running daemon: trades WebSocket for CVD, scheduled polling for market context and wallet positions. Survives restarts, detects and logs gaps on resume.
- `show --coin ETH --tf 1h,4h,1d` — terminal table per timeframe: price change, OI change, delta and CVD slope, funding (raw, 8h-equivalent, premium, percentile), inferred liquidations if available, and the watchlist's net positioning. Define CVD slope precisely in the plan (e.g. least-squares slope of cumulative delta over the last N bars of that timeframe, N configurable) and make the definition visible in `--help`.
- `liqmap --coin ETH` — liquidation map from watchlist positions, bucketed by price around the current mark, with long, short and combined coverage and the snapshot time.
- `wallets add|remove|list|discover` — manage the watchlist.
- `export --coin ETH --tf 1h --from … --to … --format csv|parquet`
- `status` — feed health, last update per stream, gaps, rate-limit headroom.

## Rate limits and reliability

- Limits I have (verify): 1000 WebSocket subscriptions per IP (one per channel per coin, no batching, counted across all connections), 100 connections, 10 unique users for user-specific subscriptions, 2000 client messages per minute, plus REST weight limits per IP (my understanding: ~1200 weight/min, with `clearinghouseState` at a low weight such as 2 and most other info requests at ~20). Per-address limits, as I understand them, apply to exchange actions, not to public info requests; confirm, and don't budget for them if they don't apply.
- Budget REST weight explicitly. Wallet polling is the main consumer. In the plan, show the arithmetic: watchlist size × poll frequency × request weight, plus market-context polling, backfill and `userFills` polling, against the per-IP limit. Make the interval and watchlist size configurable, and refuse to start (or warn loudly) if the configured combination would exceed the budget.
- Exponential backoff with jitter on errors. Pace re-subscription after reconnects so a flapping socket doesn't burn the message budget.
- Stale-feed detection per stream. Clean shutdown on SIGINT/SIGTERM with no half-written data.

## Running unattended

- Target: a small GCP Compute Engine VM (free-tier e2-micro in a US free-tier region, with a public IPv4 address; e2-small only if memory proves too tight) running the recorder 24/7 under systemd (or Docker). Wallet positioning snapshots have no history anywhere, so recorder uptime matters. `show`, `liqmap`, `export` and S3 backfills must also work on my Mac against a synced copy of the data.
- Keep the recorder within ~1 GB RAM (cap DuckDB memory, add swap). Don't run S3 backfills on the recorder VM.
- Ship closed Parquet partitions and a nightly SQLite backup to a GCS bucket; the Mac syncs from there. Keep only a configurable hot window (e.g. 90 days) on the VM disk.
- Estimate disk usage per day for raw trades, bars, market context and positioning snapshots at the proposed defaults, and make retention for raw data configurable.

## Testing

- Unit tests for: delta and CVD aggregation, funding normalization and percentile, resampling and bar alignment (including weekly Monday open), liquidation-map bucketing and coverage, idempotent upserts, gap detection.
- Recorded fixtures for payload parsing, so tests don't hit the live API.
- One opt-in live smoke test per data source.

## Out of scope for now, but design for it

- Flow-state classification per timeframe (thresholds aren't calibrated yet; this data is how I'll calibrate them).
- Alerts (e.g. funding percentile extremes, liquidation cluster near price).
- Joining with Coinglass exports for cross-venue comparison. Keep units (coin OI, 8h-equivalent funding) Coinglass-comparable to make that easy later.

## Deliverables

- Working CLI with the commands above, a sample config, and a README covering setup, running the recorder unattended, and example queries.
- A "data caveats" section in the README listing every limitation found while verifying the API: what has no history, what's inferred, what the watchlist can and can't represent. At minimum cover: no public OI history outside S3, candle depth limit, unrecoverable CVD gaps, backstop-only liquidation inference from trades, snapshot nature of cross-margin liquidation prices, vault exclusion, coverage definition, funding pinned at the interest rate, and which data sources are unofficial.
