"""Configuration loaded from a TOML file (see hlflows.example.toml)."""

from __future__ import annotations

import os
from pathlib import Path

import msgspec

from hlflows.timeutil import WEEKDAYS

DEFAULT_CONFIG_PATHS = ("hlflows.toml",)


class StorageConfig(msgspec.Struct, forbid_unknown_fields=True):
    db_path: str = "data/hlflows.sqlite"
    data_dir: str = "data"
    # Positions older than this move from SQLite to data/positions/date=*.parquet
    # (`hlflows maintain`); at 300 wallets they grow ~100 MB/day in SQLite.
    positions_hot_days: int = 14
    # `hlflows maintain --prune` deletes local Parquet (raw trades, archived positions)
    # older than this; run it only after those files are synced elsewhere. 0 = never.
    local_retention_days: int = 30


class ApiConfig(msgspec.Struct, forbid_unknown_fields=True):
    info_url: str = "https://api.hyperliquid.xyz/info"
    timeout_s: float = 20.0
    max_retries: int = 6


class RecorderConfig(msgspec.Struct, forbid_unknown_fields=True):
    ws_url: str = "wss://api.hyperliquid.xyz/ws"
    # Redundant connections, each subscribed to everything; trades are deduplicated. The
    # server closes a connection after ~3h ("Expired"), so each is replaced after
    # rotate_after_s while another one is live, and no data is lost. 0 = never rotate.
    ws_connections: int = 2
    rotate_after_s: float = 9000.0
    ping_interval_s: float = 30.0  # application-level ping; the server drops idle sockets
    dead_after_s: float = 60.0  # reconnect when nothing at all arrives for this long
    stale_trades_s: float = 300.0  # reconnect when a perp market has no trades for this long
    stale_ctx_s: float = 60.0  # reconnect when a coin's asset context stops updating
    subscribe_pause_s: float = 0.1  # between subscription messages
    reconnect_base_s: float = 1.0
    reconnect_cap_s: float = 60.0
    bar_grace_s: float = 5.0  # build a minute bar this long after the minute ends
    flush_grace_s: float = 120.0  # write an hour's raw trades to Parquet this long after it ends
    context_poll_s: float = 300.0  # REST metaAndAssetCtxs + predictedFundings
    funding_poll_offset_s: float = 90.0  # fetch fundingHistory this long after each hour
    candle_catchup_s: float = 21600.0  # refresh 1h/1d candles
    heartbeat_s: float = 10.0  # write stream health for `status`


HLP_VAULTS = [
    "0xdfc24b077bc1425ad1dea75bcb6f8158e10df303",  # HLP (parent)
    "0x010461c14e146ac35fe42271bdc1134ee31c703a",
    "0x2e3d94f0562703b25c83308a05046ddaf9a8dd14",  # liquidator (verified from its fills)
    "0x2ed5c4484ea3ff8b57d5f2fb152a40d9f2b68308",
    "0x31ca8395cf837de08b24da3f660e77761dfb974b",
    "0x469f690213c467c39a23efacfd2816896009d7d8",
    "0x5e177e5e39c0f4e421f5865a6d8beed8d921cb70",
    "0xb0a55f13d22f66e6d495ac98113841b2326e9540",
]


class WalletsConfig(msgspec.Struct, forbid_unknown_fields=True):
    # Always watched, never pruned or auto-excluded. Optional labels in `labels`.
    manual: list[str] = msgspec.field(default_factory=list)
    labels: dict[str, str] = msgspec.field(default_factory=dict)
    max_active: int = 300
    poll_interval_s: float = 120.0
    poll_concurrency: int = 4
    # REST weight per minute for discovery: address probes (clearinghouseState = 2) and at
    # most one userRole check per minute (60) for newly watched wallets.
    probe_weight_per_min: int = 120
    # A trade at least this large (USD) moves its addresses to the front of the probe queue.
    large_trade_ntl: dict[str, float] = msgspec.field(
        default_factory=lambda: {"BTC": 1e6, "ETH": 5e5, "SOL": 2.5e5, "HYPE": 2.5e5}
    )
    # A position at least this large (USD) in a coin qualifies a wallet for the watchlist.
    qualify_ntl: dict[str, float] = msgspec.field(
        default_factory=lambda: {"BTC": 5e5, "ETH": 2.5e5, "SOL": 1e5, "HYPE": 1e5}
    )
    reprobe_days: float = 7.0
    dormant_after_h: float = 24.0
    exclude_vaults: bool = True
    exclude_mm: bool = True
    # Market-maker heuristic: many fills, mostly maker, small position vs volume traded.
    mm_min_fills: int = 1000
    mm_min_maker_share: float = 0.9
    mm_max_position_to_volume: float = 0.02
    known_vaults: list[str] = msgspec.field(default_factory=lambda: list(HLP_VAULTS))
    liquidators: list[str] = msgspec.field(
        default_factory=lambda: ["0x2e3d94f0562703b25c83308a05046ddaf9a8dd14"]
    )
    # Check a shrunk position's fills for liquidations when its previous liquidation price
    # was within this distance of the mark (percent), at most N checks per round.
    liq_check_distance_pct: float = 3.0
    max_fill_checks_per_round: int = 10
    liqmap_bucket_pct: float = 0.5
    liqmap_range_pct: float = 20.0


class LiquidationProviderConfig(msgspec.Struct, forbid_unknown_fields=True):
    """Optional paid global liquidation stream. Off unless `name` and `api_key` are set."""

    name: str = ""  # adapter name registered in hlflows.liquidations.providers
    api_key: str = ""
    poll_s: float = 60.0


class MarketsConfig(msgspec.Struct, forbid_unknown_fields=True):
    coins: list[str] = msgspec.field(default_factory=lambda: ["BTC", "ETH", "SOL", "HYPE"])
    # Empty string = the canonical perp dex. HIP-3 builder dexes are ignored unless named here.
    dex: str = ""
    # Perp coin -> spot base token on Hyperliquid. Coins missing here get no spot market.
    spot_tokens: dict[str, str] = msgspec.field(
        default_factory=lambda: {"BTC": "UBTC", "ETH": "UETH", "SOL": "USOL", "HYPE": "HYPE"}
    )
    spot_quote: str = "USDC"
    # Spot markets that are a primary venue (not labelled low_significance).
    spot_primary: list[str] = msgspec.field(default_factory=lambda: ["HYPE"])


class TimeConfig(msgspec.Struct, forbid_unknown_fields=True):
    week_start: str = "monday"


class FundingConfig(msgspec.Struct, forbid_unknown_fields=True):
    percentile_window_days: int = 30


class RateLimitConfig(msgspec.Struct, forbid_unknown_fields=True):
    ip_weight_per_min: int = 1200
    # Share of the per-IP limit hlflows may use; the rest is headroom for manual use.
    ceiling_pct: float = 60.0


class ShowConfig(msgspec.Struct, forbid_unknown_fields=True):
    cvd_slope_bars: int = 20


class Config(msgspec.Struct, forbid_unknown_fields=True):
    storage: StorageConfig = msgspec.field(default_factory=StorageConfig)
    api: ApiConfig = msgspec.field(default_factory=ApiConfig)
    markets: MarketsConfig = msgspec.field(default_factory=MarketsConfig)
    time: TimeConfig = msgspec.field(default_factory=TimeConfig)
    funding: FundingConfig = msgspec.field(default_factory=FundingConfig)
    rate_limit: RateLimitConfig = msgspec.field(default_factory=RateLimitConfig)
    show: ShowConfig = msgspec.field(default_factory=ShowConfig)
    recorder: RecorderConfig = msgspec.field(default_factory=RecorderConfig)
    wallets: WalletsConfig = msgspec.field(default_factory=WalletsConfig)
    liquidation_provider: LiquidationProviderConfig = msgspec.field(
        default_factory=LiquidationProviderConfig
    )

    def validate(self) -> None:
        if self.time.week_start not in WEEKDAYS:
            raise ValueError(f"time.week_start must be one of {WEEKDAYS}")
        if not 1 <= self.recorder.ws_connections <= 5:
            raise ValueError("recorder.ws_connections must be 1-5 (Hyperliquid allows 10 per IP)")
        if not 0 < self.rate_limit.ceiling_pct <= 100:
            raise ValueError("rate_limit.ceiling_pct must be in (0, 100]")
        if self.funding.percentile_window_days < 1:
            raise ValueError("funding.percentile_window_days must be >= 1")
        for name, table in (("qualify_ntl", self.wallets.qualify_ntl),
                            ("large_trade_ntl", self.wallets.large_trade_ntl)):  # fmt: skip
            missing = set(self.markets.coins) - set(table)
            if missing:
                raise ValueError(f"wallets.{name} has no threshold for {sorted(missing)}")
        self.wallets.manual = [a.lower() for a in self.wallets.manual]
        unknown = set(self.markets.spot_tokens) - set(self.markets.coins)
        if unknown:
            raise ValueError(f"markets.spot_tokens has coins not in markets.coins: {unknown}")

    def weight_budget_lines(self) -> tuple[float, list[str]]:
        """Estimated steady-state REST weight per minute, with the arithmetic."""
        w, r = self.wallets, self.recorder
        n_perps = len(self.markets.coins)
        n_markets = n_perps + len(self.markets.spot_tokens)
        items = [
            (f"wallet polling: {w.max_active} wallets x 2 x 60/{w.poll_interval_s:g}s",
             w.max_active * 2 * 60 / w.poll_interval_s),
            (f"liquidation fill checks: {w.max_fill_checks_per_round} x ~21 per round",
             w.max_fill_checks_per_round * 21 * 60 / w.poll_interval_s),
            ("address probing (configured sub-budget)", float(w.probe_weight_per_min)),
            (f"context + predicted funding: 40 per {r.context_poll_s:g}s",
             40 * 60 / r.context_poll_s),
            (f"funding history: {n_perps} x ~21 per hour", n_perps * 21 / 60),
            (f"liquidator fills: {len(w.liquidators)} x ~21 per hour",
             len(w.liquidators) * 21 / 60),
            (f"candles: {n_markets} x 2 x ~21 per {r.candle_catchup_s:g}s",
             n_markets * 2 * 21 * 60 / r.candle_catchup_s),
        ]  # fmt: skip
        total = sum(v for _, v in items)
        return total, [f"{v:7.1f}  {name}" for name, v in items]

    @property
    def weight_ceiling(self) -> int:
        return int(self.rate_limit.ip_weight_per_min * self.rate_limit.ceiling_pct / 100)


def load_config(path: str | Path | None = None) -> Config:
    """Load config from `path`, $HLFLOWS_CONFIG, or ./hlflows.toml; defaults if none exist."""
    candidates: list[Path] = []
    if path is not None:
        candidates = [Path(path)]
    elif env := os.environ.get("HLFLOWS_CONFIG"):
        candidates = [Path(env)]
    else:
        candidates = [Path(p) for p in DEFAULT_CONFIG_PATHS]

    for candidate in candidates:
        if candidate.exists():
            cfg = msgspec.toml.decode(candidate.read_bytes(), type=Config)
            cfg.validate()
            return cfg
    if path is not None:
        raise FileNotFoundError(path)
    cfg = Config()
    cfg.validate()
    return cfg
