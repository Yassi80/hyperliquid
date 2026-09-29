# hlflows — plan (phases 1–3 and 5 done; phase 4 deferred)

Verified 2026-09-27 against the official Hyperliquid docs (gitbook), live calls to `api.hyperliquid.xyz`, and a live WebSocket capture. S3 findings come from the official docs plus two third-party write-ups; I couldn't inspect the buckets myself (see §2.4).

## 1. Stack

**Python 3.12 (managed with `uv`), asyncio for the recorder, SQLite (WAL) as the operational store, Parquet for raw trades and exports, polars as the dataframe/Parquet engine.** (Decided 2026-09-27: switched from TypeScript so a research layer can be added to the same codebase later.)

- **Language.** Python keeps the recorder and future research in one codebase: backtests and flow-state calibration can reuse the same query layer the CLI uses. Every Hyperliquid endpoint we need is a plain JSON POST or a WebSocket subscription, so we call them directly; the official Python SDK is mostly an order-signing wrapper and isn't needed.
- **Type discipline.** Payloads are decoded into typed `msgspec` structs (validation and decoding in one fast pass, low memory), and `mypy --strict` runs in tests, so field-name mistakes and API drift fail loudly.
- **Async.** The recorder is one asyncio process (WebSocket, REST polling, schedulers). SQLite writes go through a single writer task fed by a queue and batched per second, so a blocking disk write never stalls the event loop.
- **Research-ready query layer.** `hlflows.data` exposes functions like `load_bars(coin, tf, start, end)`, `load_positions(...)` and `liqmap(...)` that return polars DataFrames (`.to_pandas()` when needed). `show`, `liqmap` and `export` are thin wrappers over it, and a future `research` module and notebooks use the same functions.
- **Why not DuckDB as the only store.** DuckDB allows one read-write process *or* several read-only processes, never both at once. With `record` running 24/7, `show` and `liqmap` couldn't open the database. SQLite in WAL mode allows one writer plus concurrent readers, and `INSERT … ON CONFLICT DO UPDATE` gives idempotent upserts.
- **Why Parquet for raw trades.** Trades run about 1M/day across the configured markets (BTC ~330k, HYPE ~280k, ETH ~180k, SOL ~105k, HYPE spot ~105k). They stage in SQLite, and each closed hour is flushed to `data/trades/market=…/date=…/hour=….parquet` (zstd) with polars. That's about 30 MB/day, versus about 150 MB/day if kept in SQLite.
- **Libraries.** `websockets` (phase 2), `httpx`, stdlib `sqlite3`, `polars`, `msgspec`, `typer` + `rich` (CLI and tables), `pytest` + `pytest-asyncio`, `mypy`, `ruff`; `boto3` + `lz4` for the S3 phase only. **Changed in phase 1:** DuckDB was dropped. polars already covers resampling (calendar months, weekday-anchored weeks), Parquet read/write and DataFrames for research, so a second engine added nothing; DuckDB can still read the Parquet files for ad-hoc SQL. Python 3.12 is installed by `uv` (the Mac has 3.10 via pyenv; the VM's system Python isn't used).

## 2. Verification — what changed versus the prompt

### 2.1 Corrections
| Prompt said | Docs / live API say |
|---|---|
| 100 WS connections per IP | **10** concurrent connections and **30 new connections/min**. Subscriptions (1000), unique users (10), 2000 msgs/min confirmed; also 100 in-flight `post` messages. |
| REST weight limits "per IP and per address" | Per-IP: **1200 weight/min**. Per-address limits apply to **exchange actions only** (1 request per 1 USDC traded, open-order caps). They don't affect this tool. |
| No public trade history over REST | `recentTrades` exists, but returned **only 10 trades** live. It's useless for gap fill, so CVD gaps are still S3-only. |
| Poll `metaAndAssetCtxs` for context | `activeAssetCtx` WS sends `funding, openInterest, premium, oraclePx, markPx, midPx, impactPxs, dayNtlVlm, dayBaseVlm, prevDayPx` about once per second at zero REST weight. **Use WS as primary**, with a `metaAndAssetCtxs` REST poll every 5 min as cross-check and resume anchor. |
| `fundingHistory` = realized funding | It also returns **`premium` per hour**, so premium history (and its percentile) is backfillable. 500 rows per page. |
| `predictedFundings` = rate per venue | Also returns **`fundingIntervalHours`** per venue (HL 1h; Binance/Bybit 8h here) and `null` where a venue doesn't list the coin (HYPE on Binance/Bybit). Normalize 8h-equivalent per venue using its interval. |
| Leaderboard: "if one exists" | Unofficial `stats-data.hyperliquid.xyz/Mainnet/leaderboard`: 39 MB JSON, ~47k accounts with `accountValue` and day/week/month/allTime pnl/roi/volume, **no positions**. 3,037 accounts ≥ $1M, 459 ≥ $10M. **Not used (decided): discovery uses official sources only.** |

### 2.2 Confirmed
- **Trades WS** has `users: [buyer, seller]` and `side`. Live sample: `side:"B"` traded at the ask and `"A"` at the bid, so **`side` is the taker (aggressor) side**. The first message after subscribe is a batch of recent trades **with no `isSnapshot` flag**, so dedupe on `(coin, tid)`.
- **Candles have no taker split**: fields are `t T s i o h l c v n`. Only the **latest 5000 candles** per interval are available (1m ≈ 3.5 days, 1h ≈ 208 days, 1d ≈ 13.7 years).
- **Funding formula:** `F(8h) = P + clamp(0.01% − P, −0.05%, +0.05%)`, paid hourly at F/8, capped at 4%/h. **Funding pins at exactly 0.00125%/h whenever the 8h premium is between −0.04% and +0.06%**. All four coins were pinned when I checked, so percentile on premium is the right primary measure.
- **`clearinghouseState`** (weight 2): per position `szi` (signed), `entryPx`, `leverage {type: cross|isolated, value}`, `liquidationPx`, `positionValue`, `unrealizedPnl`, `marginUsed`, `cumFunding`; per account `marginSummary`, `crossMaintenanceMarginUsed`, `time`. Default dex only unless `dex` is passed.
- **Liquidation mechanics:** market order to book first (only 20% initially for positions over $100k, then full size after a 30s cooldown); **backstop by the liquidator vault (part of HLP)** only when equity is below 2/3 of maintenance margin.
- **`userFills`** carries `liquidation {liquidatedUser, markPx, method: market|backstop}` on liquidation fills. It returns at most 2000 recent fills, and `userFillsByTime` reaches only the latest 10,000. Cost is weight 20 plus extra per 20 items, so it's expensive; poll it on demand only (see §4).
- **HLP** vault `0xdfc2…f303` has 7 child addresses (fetched live from `vaultDetails`). One of them is the liquidator (`0x2e3d…dd14` by my recollection; the code will identify it from fill data, not from memory).

### 2.3 Spot markets (live 24h volume)
| Market | Name | 24h notional | vs perp |
|---|---|---|---|
| HYPE/USDC | `@107` | $28M | 13% of HYPE perp, **first-class** |
| USOL/USDC | `@156` | $9.8M | 5%, `low_significance` |
| UBTC/USDC | `@142` | $9.8M | 1%, `low_significance` |
| UETH/USDC | `@151` | $5.4M | 1.5%, `low_significance` |

Resolved via `spotMeta` at startup, never hard-coded. Tiny-volume pairs (HYPE/USDT0 etc.) are ignored.

### 2.4 S3 archive (requester-pays, needs AWS credentials, anonymous access refused)
- `hyperliquid-archive/asset_ctxs/<date>.csv.lz4` — asset contexts including **open interest and funding**. **This makes OI backfill possible.** I haven't confirmed the sampling interval inside each file.
- `hyperliquid-archive/market_data/<date>/<hour>/l2Book/<coin>.lz4` — L2 snapshots (not needed now).
- `hl-mainnet-node-data/node_fills` (before 2025-07-27) and `node_fills_by_block` (after) — **every fill, both sides, with addresses, `crossed` (taker flag) and liquidation fields**. That enables:
  - **CVD backfill** (sum of taker fills),
  - **exchange-wide historical liquidations, both market and backstop**. This is the one place a complete liquidation record exists, but it lags (the archive updates roughly monthly, no guarantee),
  - historical wallet discovery.
- `node_trades` is reportedly empty/dead. The two fills formats differ, so a range crossing 2025-07-27 needs both parsers.
- **Cost:** fills run about 0.8–1.0 GiB/day for all coins, and a file can't be filtered by coin before download. At $0.09/GB egress that's about **$0.08 per day of history, roughly $30 per year of history**. It's free if run inside the bucket's AWS region. `backfill --source s3` will list objects, total their sizes and print the estimated bill with a required `--yes` before downloading anything.
- Third-party option: `hydromancer-reservoir` (requester-pays Parquet) has 1s candles, all fills and **daily per-trader position snapshots**, which could backfill historical watchlist positioning. I'm noting it only; I won't build it unless you want it.

## 3. Storage schema (SQLite, all timestamps in UTC milliseconds)

```
markets(market PK, coin, kind perp|spot, hl_name, significance normal|low)
bars(market, res 1m|1h|1d, ts, o,h,l,c, volume, n, buy_sz, sell_sz, buy_ntl, sell_ntl,
        source ws|candle|s3, quality)                     PK(market, res, ts)
        -- delta = buy_sz - sell_sz per bar; 'ws' bars are never overwritten by candles
asset_ctx(coin, ts (minute), oi, mark_px, oracle_px, mid_px, premium, funding_1h,
        impact_bid, impact_ask, day_ntl_vlm, source ws|rest|s3, quality) PK(coin, ts)
funding(coin, ts (hour), rate_1h, premium (8h scale), raw_time) PK(coin, ts)  -- 8h = rate_1h*8
coverage(stream, market, start_ts, end_ts)                PK(stream, market, start_ts)
        -- where a stream is authoritative: inside, a missing bar means no trades
weight_ledger(id, ts, weight, pid, what)                  -- shared REST budget across processes
predicted_funding(coin, venue, observed_ts, rate, interval_h, next_ts) PK(coin, venue, observed_ts)
wallets(address PK, label, source manual|trades, status active|candidate|dormant, tags vault|mm,
        excluded, added_ts, last_probed_ts, qualifying_ntl, last_account_value)
addresses_seen(address PK, first_seen_ts, last_seen_ts, n_fills, taker_ntl, maker_ntl, max_trade_ntl,
        markets, last_probed_ts, probe_result)
snapshot_rounds(round_ts PK, wallets_polled, wallets_failed, weight_used, duration_ms)
positions(round_ts, address, coin, szi, entry_px, lev_type, lev, liq_px, position_value,
        upnl, margin_used, account_value)                 PK(round_ts, address, coin)
liquidations(id PK, coin, ts, address, side, sz, px, ntl, method market|backstop|unknown,
        source watched_fills|s3_fills|trades_backstop, quality)
trades_staging(market, tid, ts, side, px, sz, buyer, seller) PK(market, tid)  -- flushed hourly to Parquet
gaps(stream, market, start_ts, end_ts, reason, recoverable rest|s3|none, status open|filled|unfillable)
stream_state(stream PK, last_msg_ts, last_ok_ts, reconnects, notes)
```

- **Bar resolution: 1m** for recorded bars. It's small (1,440 rows/day/market), resamples exactly to 15m, 1h, 4h, 1d, 1w and 1M, and raw trades cover anything finer. Candle backfill also stores 1h and 1d bars, because 1m candles only reach back ~3.5 days. Each output bar uses the finest stored resolution whose coverage spans it; otherwise it's marked `partial`.
- Resampling happens in SQL at query time. The weekly anchor is configurable (default Monday 00:00 UTC); months are calendar months.
- **OI is stored in coin units**, and USD is computed at query time. Bars store per-bar delta; cumulative CVD is anchored at query time.
- Quality flags: `inferred`, `partial`, `low_significance`, `backfilled_candle_no_delta` (candle-backfilled bars have OHLCV but **no delta**, and are shown as such).
- **Positioning data:** positions are written only for configured coins and only where a position exists. At about 300 wallets every 2 minutes, that's roughly 30–50 MB/day in SQLite. Rows older than N days (configurable) are moved to monthly Parquet files. **Total disk ≈ 25–35 GB/year at defaults**, and retention is configurable.

## 4. Key mechanics
- **Recorder connections:** 1 WS connection. Subscriptions = `trades` × 5 markets + `activeAssetCtx` × 4 coins = 9 of the 1000 allowed.
  - Reconnects use exponential backoff with full jitter.
  - Re-subscribes are paced at 1 per 200ms. New connections are capped at 30/min, and the tool refuses to exceed 10 connections.
- **Stale-feed detection** runs per stream, with separate thresholds for trades (e.g. BTC silent for 60s) and asset context (silent for 10s).
- **On restart**, the tool writes a `gaps` row from `last_msg_ts` to the first new message and tags each gap's recoverability:
  - trades → S3 only,
  - asset context → S3 only, with REST available for the current value,
  - funding → REST.
- **REST weight budgeter:** a token bucket at 1200/min with a configurable ceiling (default 60%, which leaves room for manual CLI use). It's shared through a SQLite lease so `backfill` and `record` together can't exceed it. Wallet polling is the main consumer:
  - `wallets × 2 weight × (60 / interval_s)`. The default is 300 wallets every 120s = **300 weight/min (25%)**.
  - At startup the tool prints this arithmetic and **refuses to start** above the ceiling.
- **Liquidations:**
  - *Watched wallets (reliable):* when a watched wallet's position disappears or shrinks between rounds, the tool fetches `userFillsByTime` for that window only and records fills carrying `liquidation`, with its method. This costs weight only when needed.
  - *Liquidator fills (complete for backstops, but rare):* the HLP liquidator's own fills, pulled hourly (~21 weight). **Changed in phase 3:** the trades-feed inference was dropped. The liquidator's fills showed 1,998 of 2,000 recent fills were ordinary unwinds, which look identical in public trades, so that inference would have mislabelled unwinds as liquidations.
  - *S3 fills (complete, lagged):* exchange-wide, after the fact.
  - A paid-provider adapter interface exists but is off by default.
- **Vaults:** HLP and its children, plus any address `vaultDetails` identifies as a vault, are tagged `is_vault` and excluded from aggregates by default.
- **Coverage:** long coverage is watchlist long notional ÷ OI notional, and short coverage is the same for shorts; combined coverage is (long + short) ÷ (2 × OI). Shown on every positioning output, with snapshot time.
- **Funding percentile:** premium percentile over a trailing window (default 30d, mid-rank for ties). The funding percentile is shown with the share of the window pinned at the interest rate.
- **CVD slope:** least-squares slope of cumulative delta over the last N bars of the timeframe (default N=20), normalized by the mean absolute bar volume so coins compare. The definition is printed in `--help`.
- **Shutdown:** SIGINT/SIGTERM stop intake, flush the in-memory 1m bar and staging trades in one transaction, close the WS and exit. A half-written Parquet is written to `.tmp` and renamed atomically.

## 5. Commands
```
hlflows backfill --coin ETH --from 2026-01-01 [--source api|s3|all] [--yes]
hlflows record [--once]                      # daemon; --once = one poll cycle for testing
hlflows show --coin ETH --tf 1h,4h,1d [--bars 20]
hlflows liqmap --coin ETH [--bucket-pct 0.5] [--range-pct 20] [--at <ts>]
hlflows wallets add|remove|list|discover [--min-ntl 250000] [--dry-run]
hlflows export --coin ETH --tf 1h --from … --to … --format csv|parquet [--table bars|ctx|funding|positions|liqmap]
hlflows status                               # streams, last update, open gaps, weight headroom, disk
```
Discovery (decided: official sources only, no leaderboard):
- **Manual list** in config: always watched, never pruned or auto-excluded.
- **Every address in the trades feed.** The recorder upserts both sides of every trade into `addresses_seen` (fill count, taker/maker notional, largest single trade, markets). A two-minute live sample had 357 unique addresses, so expect thousands per day.
- **Probing.** A background prober calls `clearinghouseState` (weight 2) for addresses not yet probed, within its own weight sub-budget (default 60/min = 30 probes/min, ~43k/day). Addresses that just printed a large trade (per-coin thresholds, defaults BTC $1M, ETH $500k, SOL/HYPE $250k) jump the queue. Non-qualifying addresses are re-probed after 7 days, or sooner if they trade large again.
- **Promotion.** An address becomes `active` (polled every round) when its position notional in a configured coin is at or above a per-coin threshold (defaults BTC $500k, ETH $250k, SOL/HYPE $100k) and it isn't tagged `vault` or `mm`. The watchlist is capped (default 300); when full, the smallest non-manual wallets drop to `dormant`. Active wallets with no qualifying position for 24h also go `dormant`; dormant wallets are re-probed weekly.
- **Market-maker tagging.** Addresses with very high fill counts and small net position relative to their traded volume are tagged `mm` and kept out of the watchlist (thresholds configurable, reported in `wallets list --all`).
- `wallets discover` runs one probe pass on demand and shows what would be promoted (`--dry-run`).
- **Known limitation (goes in README caveats):** discovery only finds wallets that trade after the recorder starts. Large positions opened earlier and held untouched stay invisible until the wallet trades again, so coverage starts low and builds over the first weeks. The coverage share on every positioning output makes this visible.

**Weight budget at defaults (as built):** watchlist polling 300/min + liquidation fill checks ≤105/min + discovery 120/min (probes plus at most one 60-weight `userRole` vault check a minute) + context 8/min + funding/candles/liquidator ~4/min ≈ **536/min (45% of 1200)**, under the 720 ceiling. `record` refuses to start above the ceiling. The market-maker heuristic is re-applied every round, because at first probe too few fills may have been seen.

## 6. Phases
1. **Foundation (done 2026-09-27):** config, SQLite schema/migrations, REST client plus weight budgeter, msgspec payload types, the `hlflows.data` query layer, and recorded fixtures. Covers funding normalization and percentile, API backfill (1m/1h/1d candles, fundingHistory), resampling, `show` (price/funding/OI where present), and `export`. Unit tests: resampling and Monday alignment, funding and percentile, upsert idempotence, gap detection.
2. **Recorder (done 2026-09-27):** WS trades → 1m bars plus raw Parquet, `activeAssetCtx` → 1m context, predictedFundings poll, gaps, stale detection, shutdown, and `status`. Unit tests: delta/CVD aggregation, dedupe, gap-on-resume.
3. **Wallets (done 2026-09-27):** watchlist, trades-feed discovery (address capture, prober, promotion, MM tagging), vault tagging, polling rounds, positions, `liqmap`, positioning and coverage in `show`, and watched-wallet liquidations via targeted `userFillsByTime`. Unit tests: liqmap bucketing and coverage.
4. **S3 backfill (deferred, not scheduled):** cost dry-run, `asset_ctxs` → OI history, fills (both formats) → CVD bars plus exchange-wide liquidations plus historical discovery. Needs your AWS credentials.
5. **Ops and docs (done 2026-09-29):** `deploy/gcp/` (setup-vm.sh, systemd recorder service, hourly sync timer, nightly backup timer, push.sh, pull-data.sh, bucket lifecycle), `hlflows maintain` (positions older than 14 days → daily Parquet, read transparently by the query layer; local Parquet pruned after 30 days once synced), `hlflows backup` (online SQLite backup), README deployment guide and data caveats, live smoke tests per source, and the paid-liquidation adapter interface (off by default). **Changed:** no Dockerfile. The deployment is systemd on the VM, and an untested Dockerfile would be worse than none. Positions turned out to cost ~230 bytes/row in SQLite (~100 MB/day at 300 wallets), hence the 14-day hot window. You run all GCP commands yourself.

After each phase I'll stop so you can use it before I continue.

## 7. Needs your decision
1. **Stack:** decided: Python 3.12 + SQLite + Parquet/polars, as above.
2. **Where it runs 24/7:** decided: GCP free-tier e2-micro (us-central1/us-east1/us-west1) with a public IPv4 address (≈ $3.65/month if not covered by the free tier), 30 GB standard disk, and data synced to the Mac through a GCS bucket. Estimated ≈ $0–4/month.
3. **S3:** decided: deferred. No S3 backfill for now; phase 4 is parked. API backfill (candles within the 5000-candle window, full `fundingHistory`) stays in phase 1 since it's free.
4. **Wallet discovery:** decided: trades-feed addresses probed via `clearinghouseState`, plus the manual list. No leaderboard.
