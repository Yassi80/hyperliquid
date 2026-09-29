"""SQLite storage: connection setup and schema migrations.

SQLite runs in WAL mode so the recorder can write while CLI commands read.
All timestamps are UTC milliseconds. Migrations are append-only; each entry in
MIGRATIONS moves `PRAGMA user_version` up by one.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

MIGRATIONS: list[str] = [
    # 1: market data, coverage/gaps bookkeeping, REST weight ledger.
    """
    CREATE TABLE markets (
        market      TEXT PRIMARY KEY,          -- 'BTC' for perps, 'BTC/spot' for spot
        coin        TEXT NOT NULL,             -- configured coin this market belongs to
        kind        TEXT NOT NULL CHECK (kind IN ('perp', 'spot')),
        hl_name     TEXT NOT NULL,             -- name used in API calls ('BTC', '@142')
        significance TEXT NOT NULL CHECK (significance IN ('normal', 'low')),
        updated_ts  INTEGER NOT NULL
    );

    -- OHLCV bars at a stored resolution. Recorded bars (source 'ws') are 1m and carry
    -- the taker split; candle backfill (source 'candle') has no taker split.
    CREATE TABLE bars (
        market   TEXT NOT NULL,
        res      TEXT NOT NULL CHECK (res IN ('1m', '1h', '1d')),
        ts       INTEGER NOT NULL,             -- bar open
        o REAL NOT NULL, h REAL NOT NULL, l REAL NOT NULL, c REAL NOT NULL,
        volume   REAL NOT NULL,                -- base units
        n        INTEGER NOT NULL,             -- trade count
        buy_sz   REAL, sell_sz  REAL,          -- taker buy/sell volume, base units (NULL = unknown)
        buy_ntl  REAL, sell_ntl REAL,          -- taker buy/sell notional, quote units
        source   TEXT NOT NULL CHECK (source IN ('ws', 'candle', 's3')),
        quality  TEXT,          -- NULL or comma list: partial, inferred, low_significance
        PRIMARY KEY (market, res, ts)
    ) WITHOUT ROWID;

    -- Perp asset context sampled per minute. OI in coin units; USD derived at query time.
    CREATE TABLE asset_ctx (
        coin        TEXT NOT NULL,
        ts          INTEGER NOT NULL,          -- minute
        oi          REAL NOT NULL,
        mark_px     REAL NOT NULL,
        oracle_px   REAL NOT NULL,
        mid_px      REAL,
        premium     REAL,                      -- 8h-scale premium index
        funding_1h  REAL NOT NULL,             -- current hour's predicted rate
        impact_bid  REAL, impact_ask REAL,
        day_ntl_vlm REAL,
        source      TEXT NOT NULL CHECK (source IN ('ws', 'rest', 's3')),
        quality     TEXT,
        PRIMARY KEY (coin, ts)
    ) WITHOUT ROWID;

    -- Realized hourly funding. rate_1h is what's paid per hour; premium is the 8h-scale
    -- premium index averaged over the hour (both straight from fundingHistory).
    CREATE TABLE funding (
        coin     TEXT NOT NULL,
        ts       INTEGER NOT NULL,             -- settlement hour (floored)
        rate_1h  REAL NOT NULL,
        premium  REAL NOT NULL,
        raw_time INTEGER NOT NULL,             -- exact API timestamp
        PRIMARY KEY (coin, ts)
    ) WITHOUT ROWID;

    CREATE TABLE predicted_funding (
        coin        TEXT NOT NULL,
        venue       TEXT NOT NULL,             -- HlPerp, BinPerp, BybitPerp, ...
        observed_ts INTEGER NOT NULL,
        rate        REAL NOT NULL,             -- per funding interval
        interval_h  REAL NOT NULL,
        next_ts     INTEGER NOT NULL,
        PRIMARY KEY (coin, venue, observed_ts)
    ) WITHOUT ROWID;

    -- Time ranges for which a stream's data is authoritative: inside a covered range,
    -- a missing bar means "no trades", not "no data". Ranges are half-open [start, end).
    CREATE TABLE coverage (
        stream   TEXT NOT NULL,
        market   TEXT NOT NULL,
        start_ts INTEGER NOT NULL,
        end_ts   INTEGER NOT NULL,
        PRIMARY KEY (stream, market, start_ts)
    ) WITHOUT ROWID;

    -- Known holes, with why they exist and whether they can be filled.
    CREATE TABLE gaps (
        stream      TEXT NOT NULL,
        market      TEXT NOT NULL,
        start_ts    INTEGER NOT NULL,
        end_ts      INTEGER NOT NULL,
        reason      TEXT NOT NULL,
        recoverable TEXT NOT NULL CHECK (recoverable IN ('rest', 's3', 'none')),
        status      TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'filled', 'unfillable')),
        detected_ts INTEGER NOT NULL,
        PRIMARY KEY (stream, market, start_ts)
    ) WITHOUT ROWID;

    -- REST weight spent by any hlflows process, for the shared per-IP budget.
    CREATE TABLE weight_ledger (
        id     INTEGER PRIMARY KEY,
        ts     INTEGER NOT NULL,
        weight INTEGER NOT NULL,
        pid    INTEGER NOT NULL,
        what   TEXT NOT NULL
    );
    CREATE INDEX weight_ledger_ts ON weight_ledger (ts);
    """,
    # 2: recorder. Raw trades stage here until their hour is flushed to Parquet.
    """
    CREATE TABLE trades_staging (
        market TEXT NOT NULL,
        tid    INTEGER NOT NULL,
        ts     INTEGER NOT NULL,
        side   TEXT NOT NULL CHECK (side IN ('B', 'A')),   -- taker side: B buy, A sell
        px     REAL NOT NULL,
        sz     REAL NOT NULL,
        buyer  TEXT NOT NULL,
        seller TEXT NOT NULL,
        hash   TEXT NOT NULL,
        PRIMARY KEY (market, tid)
    ) WITHOUT ROWID;
    CREATE INDEX trades_staging_ts ON trades_staging (market, ts);

    -- Per-stream health for `status`: last message, last success, reconnects.
    CREATE TABLE stream_state (
        stream      TEXT PRIMARY KEY,
        last_msg_ts INTEGER,
        last_ok_ts  INTEGER,
        count       INTEGER NOT NULL DEFAULT 0,
        detail      TEXT,
        updated_ts  INTEGER NOT NULL
    ) WITHOUT ROWID;
    """,
    # 3: wallets, positions, liquidations.
    """
    -- Every address seen in the public trades feed, with activity counters and the
    -- probe schedule (clearinghouseState checks for a qualifying position).
    CREATE TABLE addresses_seen (
        address        TEXT PRIMARY KEY,
        first_seen_ts  INTEGER NOT NULL,
        last_seen_ts   INTEGER NOT NULL,
        n_fills        INTEGER NOT NULL,
        taker_ntl      REAL NOT NULL,
        maker_ntl      REAL NOT NULL,
        max_trade_ntl  REAL NOT NULL,
        large          INTEGER NOT NULL DEFAULT 0,   -- ever printed a large trade
        probe_after_ts INTEGER NOT NULL DEFAULT 0,   -- next probe due
        last_probed_ts INTEGER,
        probe_ntl      REAL                          -- configured-coin notional at last probe
    ) WITHOUT ROWID;
    CREATE INDEX addresses_seen_probe ON addresses_seen (probe_after_ts);

    CREATE TABLE wallets (
        address           TEXT PRIMARY KEY,
        label             TEXT,
        source            TEXT NOT NULL CHECK (source IN ('manual', 'trades')),
        status            TEXT NOT NULL CHECK (status IN ('active', 'dormant', 'removed')),
        tags              TEXT,               -- comma list: vault, mm, liquidator
        role              TEXT,               -- from userRole: user, vault, subAccount, ...
        added_ts          INTEGER NOT NULL,
        updated_ts        INTEGER NOT NULL,
        last_qualified_ts INTEGER,
        qualifying_ntl    REAL,
        account_value     REAL
    ) WITHOUT ROWID;

    -- Status history, so the watchlist's membership at any past time is known.
    CREATE TABLE wallet_events (
        address TEXT NOT NULL,
        ts      INTEGER NOT NULL,
        status  TEXT NOT NULL,
        reason  TEXT NOT NULL,
        PRIMARY KEY (address, ts, status)
    ) WITHOUT ROWID;

    CREATE TABLE snapshot_rounds (
        round_ts       INTEGER PRIMARY KEY,
        wallets_polled INTEGER NOT NULL,
        wallets_failed INTEGER NOT NULL,
        duration_ms    INTEGER NOT NULL
    );
    CREATE TABLE round_failures (
        round_ts INTEGER NOT NULL,
        address  TEXT NOT NULL,
        PRIMARY KEY (round_ts, address)
    ) WITHOUT ROWID;

    -- Non-zero positions in configured coins, per polling round.
    CREATE TABLE positions (
        round_ts       INTEGER NOT NULL,
        address        TEXT NOT NULL,
        coin           TEXT NOT NULL,
        szi            REAL NOT NULL,        -- signed size, coin units
        entry_px       REAL NOT NULL,
        position_value REAL NOT NULL,        -- USD at mark
        lev_type       TEXT NOT NULL,        -- cross | isolated
        lev            REAL NOT NULL,        -- leverage setting
        eff_lev        REAL,                 -- effective leverage (see wallets/poll.py)
        liq_px         REAL,                 -- NULL: no liquidation price
        upnl           REAL NOT NULL,
        margin_used    REAL NOT NULL,
        account_value  REAL NOT NULL,
        PRIMARY KEY (round_ts, address, coin)
    ) WITHOUT ROWID;
    CREATE INDEX positions_coin ON positions (coin, round_ts);

    CREATE TABLE liquidations (
        id      TEXT PRIMARY KEY,            -- source:tid:address
        coin    TEXT NOT NULL,
        ts      INTEGER NOT NULL,
        address TEXT NOT NULL,               -- liquidated account
        side    TEXT NOT NULL CHECK (side IN ('long', 'short')),  -- liquidated position
        sz      REAL NOT NULL,
        px      REAL NOT NULL,
        ntl     REAL NOT NULL,
        method  TEXT NOT NULL CHECK (method IN ('market', 'backstop', 'unknown')),
        source  TEXT NOT NULL CHECK (source IN ('watched_fills', 'liquidator_fills', 's3_fills',
                                              'provider')),
        quality TEXT NOT NULL DEFAULT 'partial'
    ) WITHOUT ROWID;
    CREATE INDEX liquidations_coin ON liquidations (coin, ts);
    """,
]


def connect(path: str | Path) -> sqlite3.Connection:
    """Open the database, creating it and applying pending migrations."""
    path = Path(path)
    if str(path) != ":memory:":
        path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=30.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 30000")
    migrate(conn)
    return conn


def migrate(conn: sqlite3.Connection) -> None:
    version: int = conn.execute("PRAGMA user_version").fetchone()[0]
    for i, sql in enumerate(MIGRATIONS[version:], start=version + 1):
        conn.execute("BEGIN IMMEDIATE")
        try:
            # executescript would COMMIT our transaction; run statements one by one instead.
            for stmt in _split_sql(sql):
                conn.execute(stmt)
            conn.execute(f"PRAGMA user_version = {i}")
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise


def _split_sql(script: str) -> list[str]:
    statements: list[str] = []
    buf = ""
    for line in script.splitlines(keepends=True):
        buf += line
        if sqlite3.complete_statement(buf):
            stmt = buf.strip()
            if stmt:
                statements.append(stmt)
            buf = ""
    if buf.strip():
        raise ValueError(f"incomplete SQL statement in migration: {buf.strip()[:80]}")
    return statements
