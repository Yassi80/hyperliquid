# hlflows

Hyperliquid flows data for higher-timeframe trading: price, open interest, taker delta / CVD,
funding and (from phase 3) watchlist positioning and liquidation maps. Public data only; no
keys, no orders.

Status: **phases 1–3 and 5** (REST backfill, recorder, wallet positioning, `show`,
`liqmap`, `wallets`, `export`, `status`, GCP deployment). The S3 backfill (phase 4) is
deferred; see [PLAN.md](PLAN.md).

## Setup

Requires [uv](https://docs.astral.sh/uv/); it installs Python 3.12 itself.

```sh
uv sync
cp hlflows.example.toml hlflows.toml   # optional; every key has a default
uv run hlflows --help
```

Config is read from `--config`, then `$HLFLOWS_CONFIG`, then `./hlflows.toml`.

## Commands

```sh
# Pull all history the REST API allows, and report what couldn't be fetched and why.
uv run hlflows backfill --coin ETH --from 2026-01-01
uv run hlflows backfill --from 2026-01-01              # all configured coins

# Last closed bar per timeframe, the funding panel and cross-venue predicted funding.
uv run hlflows show --coin ETH --tf 15m,1h,4h,1d,1w,1M

# Run the recorder (foreground; stops cleanly on Ctrl-C / SIGTERM, safe to restart).
uv run hlflows record

# Recorder health: streams, last updates, open gaps, coverage, rate-limit headroom, disk.
uv run hlflows status

# Liquidation map of watched positions around the mark, with coverage of open interest.
uv run hlflows liqmap --coin ETH
uv run hlflows liqmap --coin BTC --bucket-pct 1 --range-pct 30 --at 2026-10-01T12:00

# The watchlist (the recorder fills it; manual wallets are always watched).
uv run hlflows wallets list [--all]
uv run hlflows wallets add 0xabc... --label "some whale"
uv run hlflows wallets remove 0xabc...
uv run hlflows wallets discover --dry-run   # probe due addresses now and show the outcome

# Export. Resampled to --tf: bars (perp), spot, ctx (OI/mark/premium), funding.
# Per polling round: positioning. Per event: liquidations. At the last round before --to: liqmap.
uv run hlflows export --coin ETH --tf 1h --from 2026-09-01 --format parquet
uv run hlflows export --coin HYPE --tf 1d --from 2026-06-01 --table funding --out hype_funding.csv
uv run hlflows export --coin BTC --tf 1h --from 2026-10-01 --table positioning

# Self-contained HTML dashboard (all coins; 15m/1h/4h/1d; 200 bars each).
uv run hlflows dashboard --open

# Housekeeping (the GCP timers run these): archive positions older than
# storage.positions_hot_days to Parquet; --prune also deletes local Parquet older than
# storage.local_retention_days (only once it's synced elsewhere). Online DB backup.
uv run hlflows maintain [--prune]
uv run hlflows backup /path/to/copy.sqlite
```

Backfill runs within the shared REST weight budget (`rate_limit.ceiling_pct` of the 1200/min
per-IP limit, 60% by default), so a full backfill of all four coins takes a few minutes.
Re-running it is safe: every write is an idempotent upsert.

## The recorder

`record` runs one process with two redundant WebSocket connections, each subscribed to
`trades` for every perp and spot market and `activeAssetCtx` for every perp (24 of the
1,000 subscriptions and 2 of the 10 connections allowed per IP), plus REST polling.
Trades are deduplicated by id, and a minute counts as recorded if either connection was
live for all of it. Hyperliquid closes every connection after about 3 hours (close code
1000, reason "Expired"); the recorder replaces each connection after 2.5 hours, one at a
time and only while the other has been healthy for a minute, so the expiry never costs
data. An unexpected drop of one connection is covered by the other.

| What | How | Stored as |
|---|---|---|
| Trades | WebSocket, deduplicated by trade id | 1m bars with taker buy/sell split; raw trades in `data/trades/market=…/date=…/hour=HH.parquet` |
| OI, mark, oracle, premium, predicted funding | `activeAssetCtx` WebSocket (~1/s) | last sample per minute in `asset_ctx` |
| Context cross-check, cross-venue predicted funding | REST every 5 min | `asset_ctx` (never overrides a WebSocket sample), `predicted_funding` |
| Realized funding | REST `fundingHistory` 90 s after each hour | `funding` |
| 1h/1d candles | REST every 6 h | `bars` |
| OHLCV for gaps | REST 1m candles right after a reconnect | `bars` (no taker split) |

On every (re)connect the recorder compares what's covered with where the new live
window starts and records the difference in `gaps`, with whether it can be filled: OHLCV
from 1m candles (`rest`, done automatically if within ~3.5 days) or the taker split only
from the S3 archive (`s3`). `status` lists open gaps.

Reconnects use exponential backoff with jitter, and subscriptions are paced. A
connection that goes silent for 60 s, a perp market with no trades for 5 min on that
connection, or an asset context that stops for 60 s forces that connection to reconnect.
`status` shows `ws` with unexpected drops as the count and "2/2 connected, N rotations". Memory use is about 80 MB.

## Wallet positioning

Hyperliquid exposes every account's positions, leverage and liquidation price. The
recorder keeps a watchlist and polls it:

- **Discovery (official data only).** Both addresses of every trade go into
  `addresses_seen`. A prober checks each new address once with `clearinghouseState` and
  watches it if it holds at least `wallets.qualify_ntl` in a configured coin (defaults
  BTC $500k, ETH $250k, SOL/HYPE $100k). Addresses that just made a large trade go first.
  Others are re-checked weekly. Discovery spends at most `wallets.probe_weight_per_min`
  (120/min: ~30 probes plus one `userRole` check).
- **Watchlist.** Capped at `wallets.max_active` (300). When full, a bigger new wallet
  displaces the smallest discovered one. A discovered wallet with no qualifying position
  for 24 h goes dormant (re-checked weekly). Manual wallets (`wallets.manual` in the
  config, or `wallets add`) are always polled.
- **Exclusions.** Vaults (HLP and its sub-accounts; anything `userRole` reports as a vault)
  and likely market makers (≥1000 fills seen, ≥90% maker, position ≤2% of volume traded)
  are tagged and left out of aggregates by default (`exclude_vaults`, `exclude_mm`).
- **Polling.** Every 2 minutes each watched wallet's positions in the configured coins go
  into `positions` (size, entry, leverage setting, effective leverage, liquidation price,
  uPnL, account value). Status changes go into `wallet_events`, so the watchlist's
  membership at any past time is known.
- **Liquidations.** When a watched position shrinks or disappears while its previous
  liquidation price was within 3% of the mark, the recorder pulls that wallet's fills
  and stores any with a `liquidation` record (`method` market or backstop). Separately,
  the HLP liquidator's own fills are pulled hourly: every backstop liquidation,
  exchange-wide.

At the defaults the recorder budgets about 536 REST weight/min of the 720 ceiling. `status`
prints the arithmetic, and `record` refuses to start if the configuration exceeds it.

## Running unattended on GCP

The recorder runs 24/7 on a free-tier **e2-micro** VM (2 shared vCPUs, 1 GB RAM, 30 GB
standard disk) under systemd. Raw trades and archived positions sync hourly to a Cloud
Storage bucket, and the database is backed up nightly. You pull data to your Mac from the
bucket. Everything below runs from your Mac unless it says otherwise.

What runs on the VM:

| Unit | When | What |
|---|---|---|
| `hlflows-recorder.service` | always (restarts on failure) | `hlflows record` |
| `hlflows-sync.timer` | hourly at :10 | archive positions older than 3 days to Parquet → `gcloud storage rsync` trades and positions to the bucket → delete local Parquet older than 30 days |
| `hlflows-dashboard.timer` | every 5 min | rebuild the dashboard page served by Caddy (if set up, see below) |
| `hlflows-backup.timer` | daily 01:30 UTC | online SQLite backup → gzip → `gs://BUCKET/backups/hlflows-DATE.sqlite.gz` and `latest.sqlite.gz` (dated copies deleted after 14 days) |

Layout on the VM: code in `/opt/hlflows`, Python and virtualenv in `/opt/hlflows-runtime`,
data in `/var/lib/hlflows`, config in `/etc/hlflows/hlflows.toml`, all run as the
`hlflows` system user. A `hlflows` wrapper command runs the CLI as that user.

### 1. One-time GCP setup

Prerequisites: the [gcloud CLI](https://cloud.google.com/sdk/docs/install) on your Mac and a
project with billing enabled (the free tier still requires a billing account).

```sh
cp deploy/gcp/gcp.env.example deploy/gcp/gcp.env   # fill in PROJECT, BUCKET (region/zone ok)
source deploy/gcp/gcp.env

gcloud auth login
gcloud config set project "$PROJECT"
gcloud services enable compute.googleapis.com storage.googleapis.com

# Bucket in the VM's region (VM → bucket traffic in the same region is free).
gcloud storage buckets create "gs://$BUCKET" --location="$REGION" \
  --default-storage-class=STANDARD --uniform-bucket-level-access --public-access-prevention
gcloud storage buckets update "gs://$BUCKET" --lifecycle-file=deploy/gcp/bucket-lifecycle.json

# A service account for the VM that can only write objects in this bucket.
gcloud iam service-accounts create hlflows-vm --display-name="hlflows recorder VM"
gcloud storage buckets add-iam-policy-binding "gs://$BUCKET" \
  --member="serviceAccount:hlflows-vm@$PROJECT.iam.gserviceaccount.com" \
  --role=roles/storage.objectAdmin
gcloud storage buckets add-iam-policy-binding "gs://$BUCKET" \
  --member="serviceAccount:hlflows-vm@$PROJECT.iam.gserviceaccount.com" \
  --role=roles/storage.legacyBucketReader

# The VM: free-tier machine type and disk, Debian 12, ephemeral public IP.
gcloud compute instances create "$VM" --zone="$ZONE" \
  --machine-type=e2-micro \
  --image-family=debian-12 --image-project=debian-cloud \
  --boot-disk-size=30GB --boot-disk-type=pd-standard \
  --service-account="hlflows-vm@$PROJECT.iam.gserviceaccount.com" \
  --scopes=cloud-platform
```

Optional, recommended:

```sh
# A budget alert so a misconfiguration can't run up a bill unnoticed.
gcloud services enable billingbudgets.googleapis.com
gcloud billing accounts list        # note the ACCOUNT_ID
gcloud billing budgets create --billing-account=ACCOUNT_ID --display-name=hlflows \
  --budget-amount=10USD --threshold-rule=percent=0.5 --threshold-rule=percent=1.0

# Allow SSH only through Google's IAP proxy instead of from the whole internet
# (then add --tunnel-through-iap to the gcloud compute ssh/scp commands in push.sh).
gcloud compute firewall-rules create allow-ssh-iap --network=default \
  --allow=tcp:22 --source-ranges=35.235.240.0/20
gcloud compute firewall-rules delete default-allow-ssh
```

### 2. Deploy

`push.sh` copies the **committed** code (`git HEAD`) to the VM and runs
`deploy/gcp/setup-vm.sh` there. That script adds 1 GB of swap, installs uv and Python 3.12,
creates the user, directories and config, installs the systemd units and (re)starts the
recorder. Re-run it after every commit you want deployed; it's idempotent and keeps the
config and data.

```sh
git commit ...                     # push.sh deploys HEAD only
bash deploy/gcp/push.sh
```

Then backfill history once (candles and funding; runs alongside the recorder within the
shared rate-limit budget, a few minutes):

```sh
gcloud compute ssh "$VM" --zone="$ZONE" -- hlflows backfill --from 2026-01-01
```

### 3. Operate

```sh
# Health: recorder state, streams, gaps, watchlist, rate-limit budget, disk.
gcloud compute ssh "$VM" --zone="$ZONE" -- hlflows status
gcloud compute ssh "$VM" --zone="$ZONE" -- hlflows show --coin ETH --tf 1h,4h,1d
gcloud compute ssh "$VM" --zone="$ZONE" -- hlflows liqmap --coin BTC

# Logs, timers.
gcloud compute ssh "$VM" --zone="$ZONE" -- journalctl -u hlflows-recorder -n 100 --no-pager
gcloud compute ssh "$VM" --zone="$ZONE" -- systemctl list-timers 'hlflows-*'

# Config changes (manual wallets, thresholds): edit, then restart.
gcloud compute ssh "$VM" --zone="$ZONE" -- sudo nano /etc/hlflows/hlflows.toml
gcloud compute ssh "$VM" --zone="$ZONE" -- sudo systemctl restart hlflows-recorder

# Stop / start the recorder (it stops cleanly on SIGTERM; restarts record the gap).
gcloud compute ssh "$VM" --zone="$ZONE" -- sudo systemctl stop hlflows-recorder
gcloud compute ssh "$VM" --zone="$ZONE" -- sudo systemctl start hlflows-recorder
```

### 4. Pull data to your Mac

```sh
bash deploy/gcp/pull-data.sh ~/hlflows-data     # raw trades, archived positions, nightly DB
HLFLOWS_CONFIG=~/hlflows-data/hlflows.toml uv run hlflows show --coin ETH
```

The database copy is the latest nightly backup (up to a day old); query the VM for live
views. Raw trades and archived positions sync incrementally.

### 5. Dashboard on your phone (HTTPS + password)

The VM rebuilds the dashboard every 5 minutes (`hlflows-dashboard.timer`, low priority,
capped at 500 MB) into `/var/lib/hlflows-www/index.html`. Caddy serves it over HTTPS
with a Let's Encrypt certificate and a username/password. The page reloads itself when a
newer build is due, keeps your coin/timeframe/view in the URL, and warns when it's stale.

One-time setup:

1. **A hostname.** Sign in at [duckdns.org](https://www.duckdns.org), create a subdomain
   (e.g. `yourname`) and set its IP to the VM's external IP. (DuckDNS is on the Public
   Suffix List, so Let's Encrypt issues its certificates reliably; sslip.io/nip.io are not.)
2. **Keep the IP and open ports 80/443** (80 is for certificate issuance and the redirect):

   ```sh
   source deploy/gcp/gcp.env
   IP=$(gcloud compute instances describe "$VM" --zone="$ZONE" \
        --format='get(networkInterfaces[0].accessConfigs[0].natIP)')
   gcloud compute addresses create hlflows-ip --addresses="$IP" --region="$REGION"
   gcloud compute firewall-rules create allow-hlflows-web --network=default \
     --allow=tcp:80,tcp:443 --target-tags=hlflows-web --source-ranges=0.0.0.0/0
   gcloud compute instances add-tags "$VM" --zone="$ZONE" --tags=hlflows-web
   ```

   A static address costs the same as the ephemeral one while the VM uses it; it just
   survives VM stop/start, so the DuckDNS name stays valid.
3. **Set `WEB_HOST` and `WEB_USER`** in `deploy/gcp/gcp.env`, deploy (`bash
   deploy/gcp/push.sh`, which installs the dashboard timer), then:

   ```sh
   bash deploy/gcp/web-setup.sh    # asks for a password (or generates one); installs Caddy
   ```

   Re-run it any time to change the password. Only a bcrypt hash is stored on the VM.
4. On your phone open `https://yourname.duckdns.org`, sign in, and "Add to Home screen".

On a phone, charts don't zoom on drag (so the page scrolls normally); tap a chart to see
every panel's values at that time.

Security: the password travels only over HTTPS. There's no rate limit on login attempts,
so use a long random password (the generated one is 20 characters). Caddy logs requests
to the VM's journal (`journalctl -u caddy`). The page holds market data and your
watchlist analysis, no keys or credentials. To take it offline:
`gcloud compute ssh "$VM" --zone="$ZONE" -- sudo systemctl disable --now caddy`.

### 6. Alerts on your phone (rules + Claude)

After each 5-minute dashboard build the VM runs `hlflows alerts`. Rules check the fresh
flows data, and new alerts go to your phone at once through [ntfy](https://ntfy.sh).
Optionally, the VM then starts a Claude Code routine on your Claude subscription. The
routine reads the alerts plus a compact snapshot and sends a second push with a written
read of the flows.

| Rule | Fires when (defaults, all in `[alerts]`) |
|---|---|
| `cvd_divergence` | price moved ≥1% / 2% / 4% (1h / 4h / 1d) over the last 6 closed bars while perp takers leaned ≥5% of volume the other way |
| `spot_perp_divergence` | spot and perp takers lean opposite ways (≥5% each), primary spot markets only (HYPE) |
| `oi_change` | open interest moved ≥3% / 5% / 8% in the last closed bar, labelled long build / short build / covering / unwind |
| `premium_extreme` | premium percentile ≥95 or ≤5 vs its trailing window |
| `liq_cluster` | watched liquidations within 3% of the mark ≥ $25M BTC / $15M ETH / $5M SOL, HYPE |
| `liquidations` | liquidations seen in the last closed hour ≥ $5M BTC / $3M ETH / $1M SOL, HYPE |
| `watch_flow` | the same watched wallets moved ≥ $20M BTC / $10M ETH / $5M SOL, HYPE net in a 4h or 1d bar |
| `health` | recorder down, no trade data or stale dashboard for 15 min (urgent priority) |

Rules use closed bars only. A rule doesn't repeat for the same coin, timeframe and
direction within 2 bars (at least an hour); coin-level rules wait 4 hours. Every alert,
with the numbers behind it, is stored in the `alerts` table (`hlflows alerts --history 20`),
so thresholds can later be checked against what price did next. The labels are
heuristics: none of the thresholds are calibrated yet.

Setup:

1. **ntfy:** install the ntfy app on your phone and subscribe to a long random topic
   name (e.g. from `openssl rand -hex 12`). The topic name is the secret: anyone who
   knows it can read the channel.
2. **Config on the VM** (`/etc/hlflows/hlflows.toml`), then test:
   ```toml
   [alerts]
   ntfy_topic = "hlflows-<random>"
   dashboard_url = "https://yourname.duckdns.org"
   ```
   ```sh
   gcloud compute ssh "$VM" --zone="$ZONE" -- hlflows alerts --test-push
   gcloud compute ssh "$VM" --zone="$ZONE" -- hlflows alerts --dry-run   # what would fire now
   ```
3. **Optional Claude read:** create a routine at claude.ai/code/routines.
   - **Prompt:** "Follow deploy/routine/prompt.md in the repository."
   - **Repository:** this repo (pushed to a private GitHub repo).
   - **Environment:** network access Custom with `ntfy.sh` allowed, and an `NTFY_TOPIC`
     credential/variable.
   - **Trigger:** API. Put the URL and token into `alerts.routine_fire_url` and
     `alerts.routine_token`, then check with `hlflows alerts --test-routine`.

   Routine runs count against your Claude plan's usage; the VM starts at most one per
   15 minutes and only when market alerts fire.

### Disk and cost

At the defaults (300 watched wallets, 4 coins), per day: raw trades ~36 MB of Parquet,
positions ~100 MB in SQLite for the 14-day hot window (~1.4 GB steady state) and roughly
10–25 MB/day once archived to Parquet, bars and context a few MB. The VM keeps 30 days of
Parquet, so its disk use levels off around 5–6 GB including the OS; the bucket grows by
roughly 15–20 GB a year (~$0.30–0.40/month at $0.02/GB-month after the free 5 GB). The VM
and disk are free-tier; the public IPv4 address may cost ~$3.65/month.

## Dashboard

`hlflows dashboard` writes one self-contained HTML page with every configured coin and
timeframe. Pick the coin and timeframe in the page's filter row, or bookmark a view
(`dashboard.html#coin=ETH&tf=4h`). The page loads its charting library (Plotly) from the
internet.

- **Tiles:** price, funding (hourly, 8h equivalent, premium, premium percentile, share of
  hours pinned at the interest rate), watchlist positioning with coverage, liquidations.
- **Last closed bar per timeframe** (15m to 1w): price, OI, delta, CVD slope, spot delta,
  funding, premium, watchlist Δnet and liquidations, with ▲/▼ for direction.
- **Stacked charts** sharing one time axis, one panel per measure: price candles, open
  interest, perp CVD, spot CVD, funding and premium, watchlist long/short notional,
  watchlist net flow (same wallets, cumulative), liquidations. Hover shows every panel's
  value at that time; drag to zoom. A **Table** view lists the same numbers per bar.
- **Liquidation map** of watched positions around the mark.

Colours mean the same thing everywhere: blue = buy / long / up, red = sell / short / down.

```sh
bash deploy/gcp/dashboard.sh                  # build on the VM from live data, open on your Mac
bash deploy/gcp/dashboard.sh --bars 400 --tf 1h,4h,1d
bash deploy/gcp/pull-and-dashboard.sh         # fresh backup → pull everything to ~/hlflows-data → build locally
bash deploy/gcp/pull-and-dashboard.sh ~/hlflows-data --bars 400 --tf 1h,4h,1d
HLFLOWS_CONFIG=~/hlflows-data/hlflows.toml uv run hlflows dashboard --open   # from pulled data
```

## Using the data from Python

The CLI reads through `hlflows.data`, which returns polars DataFrames; research code can use
the same functions:

```python
from hlflows import db, data
from hlflows.timeutil import TIMEFRAMES, parse_date

conn = db.connect("data/hlflows.sqlite")
bars = data.load_bars(conn, "ETH", TIMEFRAMES["4h"], parse_date("2026-06-01"))
funding = data.load_funding_bars(conn, "ETH", TIMEFRAMES["1d"], parse_date("2026-06-01"))
cvd = bars.with_columns(bars["delta"].cum_sum().alias("cvd"))  # anchor CVD at any start
```

`load_bars` columns: `ts o h l c volume n buy_sz sell_sz delta buy_ntl sell_ntl res complete
taker closed quality`. `res` is the stored resolution the bar was built from; `complete` is
false when stored data doesn't span the whole bar; `taker` says whether the taker split
(delta) is known for the whole bar.

## Conventions

- All timestamps are UTC milliseconds. Intraday bars align to the epoch (4h bars open at
  00, 04, 08... UTC); daily bars at 00:00 UTC; weekly bars at 00:00 UTC on `time.week_start`
  (Monday by default); monthly bars on the 1st.
- Open interest is stored in coin units; USD is derived at query time.
- Per-bar delta (taker buy − taker sell, coin units) is stored, never cumulative CVD.
- Funding: `rate_1h` is what Hyperliquid pays per hour; `rate_8h` = `rate_1h` × 8 for
  comparison with centralized venues (Coinglass-comparable). `premium` is Hyperliquid's
  8h-scale premium index, stored as reported.
- Quality flags on rows: `partial`, `low_significance`, and from later phases `inferred`.

## Data caveats

This list grows as later phases add data sources.

- **Funding pins at the interest rate.** Hyperliquid funding is
  `F_8h = P + clamp(0.01% − P, −0.05%, +0.05%)`, paid hourly as `F_8h / 8`, capped at 4%/h
  (verified exactly against live `fundingHistory`). Whenever the 8h premium P is between
  −0.04% and +0.06%, funding is exactly +0.00125%/h: in the last 30 days HYPE was pinned
  72% of hours. So a funding percentile is mostly ties. `show` reports the **premium
  percentile** as the primary measure; the funding percentile uses mid-rank ties, shown
  with the pinned share.
- **Candle depth.** `candleSnapshot` serves only the latest ~5000 candles per interval:
  about 3.5 days of 1m, 208 days of 1h, 13 years of 1d. Older 1m/1h history is recorded as
  a gap (recoverable only from the S3 archive, which is deferred). Output bars fall back
  to coarser candles automatically and say which resolution they used.
- **No taker split in candles.** Candle-built bars have no delta; delta and CVD come only
  from the recorder and exist only for minutes it was live. Gaps in the taker split can't
  be filled from the API (the only REST trade endpoint, `recentTrades`, returns ~10 trades);
  only the S3 archive could, which is deferred.
- **Coverage is conservative.** A minute counts as recorded only if the recorder was live
  for all of it *and* a later trade proved the stream was still delivering. The first
  minute after a (re)connect and the last before a disconnect are stored but flagged
  `partial`; 1m candles then replace their OHLCV. For thin spot markets the proof can lag
  by minutes.
- **Bars with a few missing minutes keep their delta.** When no connection is live (both
  dropped at once, a restart or a deploy) about a minute of taker split is lost. A bar containing one still sums every recorded minute, with `taker_cov`
  (share of the bar covered, 0–1) and the `taker_partial` quality flag; `show` and the
  dashboard mark such deltas with ≈. Delta is empty only for bars with no recorded
  minute at all.
- **The replay on subscribe isn't counted as coverage.** The trades channel resends recent
  trades on every subscribe; they are stored (deduplicated) but don't extend coverage
  backwards, because how far back they reach isn't documented.
- **Late trades after an hour is archived.** A trade arriving more than 2 minutes after its
  hour ended goes into the Parquet file but not into that minute's bar (logged). This hasn't
  been observed; the setting is `recorder.flush_grace_s`.
- **Local clock.** Live windows use the recorder's clock (trade and bar times are the
  exchange's). Keep NTP on; the one-minute rounding absorbs normal skew.
- **No open-interest history.** The API only gives current OI. OI history starts when the
  recorder starts (a `backfill` stores one current sample). The S3 archive has daily asset
  context files that could fill this; deferred.
- **Wrapped spot markets are thin.** BTC, ETH and SOL spot on Hyperliquid are wrapped tokens
  (UBTC, UETH, USOL; 1–5% of perp volume) and are labelled `low_significance`. HYPE/USDC
  is HYPE's primary spot market and is not.
- **The watchlist is a sample, and it starts empty.** Discovery only sees wallets that trade
  while the recorder runs. A large position opened earlier and held without trading stays
  invisible until its owner trades again, so coverage starts low and builds over weeks.
  Every positioning output shows coverage: long notional ÷ OI notional, short notional ÷ OI
  notional, and combined (long + short) ÷ (2 × OI), because OI counts one side.
- **Liquidation prices are estimates at snapshot time.** For cross margin they depend on the
  whole account and move with price, other positions and transfers. Positions without a
  liquidation price (well-collateralized accounts) are counted separately.
- **Effective leverage:** isolated = position value ÷ its margin; cross = the account's total
  cross notional ÷ account value. The leverage *setting* is stored too (`lev`).
- **Liquidations are partial.** Hyperliquid has no public liquidation feed. Most liquidations
  are market orders into the book and are invisible in public trades, except for watched
  wallets (via their fills). Backstop liquidations (the liquidator vault taking over) are
  complete but rare: 2 in the ~96 days of the liquidator's recent fills, none in BTC/ETH/
  SOL/HYPE. Trades with the liquidator as counterparty are *not* used: most are it
  unwinding positions, not liquidations. All liquidation rows are labelled `partial`.
- **Watch Δnet in `show`** compares the same wallets at both ends of the bar (polled
  successfully in both rounds), so watchlist membership changes don't read as flow. It's
  converted to USD at the bar's close.
- **Vault/MM exclusion is a heuristic.** Vaults are identified by `userRole` (rate-limited
  to one check a minute, so a new wallet may be counted for a minute or two before being
  tagged); market makers by the activity thresholds above.
- **Backups and pulled copies lag.** The nightly backup (and the database `pull-data.sh`
  fetches) is up to a day old. Positions older than 3 days live in
  `data/positions/date=*.parquet`, not in SQLite; the query layer reads both.
- **Predicted funding for other venues** comes from Hyperliquid's `predictedFundings`
  (Binance, Bybit, Hyperliquid) and exists only as snapshots; venues that don't list a
  coin are omitted.

## Development

```sh
uv run pytest                                  # unit tests on recorded fixtures, no network
HL_LIVE=1 uv run pytest tests/live             # opt-in smoke tests against the real API
uv run mypy && uv run ruff check src tests scripts
uv run python scripts/record_fixtures.py       # refresh tests/fixtures from the live API
```
