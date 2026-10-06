"""Alert rules: pure functions over the flows payload (flows.json, see `dashboard --json`).

Each rule looks at closed bars only and returns `Alert`s; cooldowns, storage and delivery
live in `engine`. The labels the rules attach ("short build", "absorption?") are
heuristics with uncalibrated thresholds: the numbers behind each alert are kept in
`facts`, so alerts can later be checked against what price did next.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from typing import Any

from hlflows.config import AlertsConfig

Payload = dict[str, Any]


@dataclass
class Alert:
    rule: str
    coin: str
    tf: str  # timeframe, or "-" for coin-level rules
    direction: str
    bar_ts: int | None  # the bar / snapshot the alert refers to
    priority: int  # ntfy priority, 1 (min) - 5 (max)
    title: str
    message: str
    facts: dict[str, Any] = field(default_factory=dict)


def _num(v: Any) -> float | None:
    return float(v) if isinstance(v, int | float) else None


def _usd(v: float) -> str:
    a = abs(v)
    for div, suffix in ((1e9, "B"), (1e6, "M"), (1e3, "k")):
        if a >= div:
            return f"{'-' if v < 0 else ''}${a / div:,.1f}{suffix}"
    return f"{'-' if v < 0 else ''}${a:,.0f}"


def _closed(series: Payload) -> list[int]:
    return [i for i, c in enumerate(series.get("closed") or []) if c]


@dataclass(frozen=True)
class Window:
    """The last `n` closed bars: from the close of bar i0 to the close of bar i1."""

    i0: int
    i1: int
    bars: list[int]  # indices of the bars in (i0, i1]


def _window(series: Payload, n: int) -> Window | None:
    closed = _closed(series)
    if len(closed) < n + 1:
        return None
    return Window(closed[-1 - n], closed[-1], closed[-n:])


def _taker_share(
    series: Payload, w: Window, cvd_key: str, vol_key: str, cov_key: str, min_cov: float
) -> float | None:
    """Net taker flow over the window as a share of volume: +1 all buying, -1 all selling.
    None if the taker split is missing for too much of any bar in the window."""
    cvd, vol, cov = series.get(cvd_key) or [], series.get(vol_key) or [], series.get(cov_key) or []
    a, b = (
        _num(cvd[w.i0]) if w.i0 < len(cvd) else None,
        _num(cvd[w.i1]) if w.i1 < len(cvd) else None,
    )
    if a is None or b is None:
        return None
    for i in w.bars:
        c = _num(cov[i]) if i < len(cov) else None
        if c is None or c < min_cov:
            return None
    volume = sum(_num(vol[i]) or 0.0 for i in w.bars)
    return (b - a) / volume if volume > 0 else None


def _pct_change(series: Payload, key: str, i0: int, i1: int) -> float | None:
    vals = series.get(key) or []
    a, b = _num(vals[i0]), _num(vals[i1])
    return (b / a - 1) * 100 if a and b is not None else None


def _tf_series(payload: Payload, coin: str, tf: str) -> Payload | None:
    s: Payload | None = payload["coins"][coin].get("tfs", {}).get(tf)
    return s if s and s.get("ts") else None


# ---------------------------------------------------------------------------------- rules


def premium_extreme(payload: Payload, coin: str, cfg: AlertsConfig) -> Iterator[Alert]:
    f = payload["coins"][coin].get("funding")
    if not f or _num(f.get("premium_pct")) is None:
        return
    pct, prem = float(f["premium_pct"]), _num(f.get("premium")) or 0.0
    facts = {"premium_pct": pct, "premium_8h": prem, "rate_8h": f.get("rate_8h"),
             "pinned_share": f.get("pinned_share"), "settled": f.get("at")}  # fmt: skip
    if pct >= cfg.premium_pct_high:
        yield Alert("premium_extreme", coin, "-", "high", f.get("ts"), 3,
                    f"{coin} premium extreme: {pct:.0f}th pct",
                    f"Premium {prem * 100:+.4f}% (8h) is at the {pct:.0f}th percentile of its "
                    "trailing window: longs paying up, crowded-long risk.", facts)  # fmt: skip
    elif pct <= cfg.premium_pct_low:
        yield Alert("premium_extreme", coin, "-", "low", f.get("ts"), 3,
                    f"{coin} premium extreme: {pct:.0f}th pct",
                    f"Premium {prem * 100:+.4f}% (8h) is at the {pct:.0f}th percentile of its "
                    "trailing window: shorts paying up, crowded-short risk.", facts)  # fmt: skip


def cvd_divergence(payload: Payload, coin: str, tf: str, cfg: AlertsConfig) -> Iterator[Alert]:
    s = _tf_series(payload, coin, tf)
    w = _window(s, cfg.window_bars) if s else None
    if s is None or w is None:
        return
    price = _pct_change(s, "c", w.i0, w.i1)
    share = _taker_share(s, w, "cvd", "volume", "taker_cov", cfg.min_taker_cov)
    need = cfg.divergence_price_pct.get(tf)
    if price is None or share is None or need is None:
        return
    facts = {"price_chg_pct": price, "perp_taker_share": share, "bars": cfg.window_bars,
             "oi_chg_pct": _pct_change(s, "oi", w.i0, w.i1)}  # fmt: skip
    span = f"the last {cfg.window_bars} {tf} bars"
    s_txt = f"{abs(share) * 100:.0f}% of volume"
    if price >= need and share <= -cfg.divergence_taker_share:
        yield Alert("cvd_divergence", coin, tf, "bearish", s["ts_ms"][w.i1], 3,
                    f"{coin} {tf}: price up, perp takers selling",
                    f"Price {price:+.1f}% over {span} while perp takers sold on net ({s_txt}): "
                    "the rise isn't driven by aggressive buying (absorption / distribution?).",
                    facts)  # fmt: skip
    elif price <= -need and share >= cfg.divergence_taker_share:
        yield Alert("cvd_divergence", coin, tf, "bullish", s["ts_ms"][w.i1], 3,
                    f"{coin} {tf}: price down, perp takers buying",
                    f"Price {price:+.1f}% over {span} while perp takers bought on net ({s_txt}): "
                    "buying into the drop is being absorbed (absorption?).", facts)  # fmt: skip


def spot_perp_divergence(
    payload: Payload, coin: str, tf: str, cfg: AlertsConfig
) -> Iterator[Alert]:
    if payload["coins"][coin].get("spot_significance") != "normal":
        return  # thin wrapped spot markets (UBTC, UETH, USOL) aren't a meaningful read
    s = _tf_series(payload, coin, tf)
    w = _window(s, cfg.window_bars) if s else None
    if s is None or w is None:
        return
    perp = _taker_share(s, w, "cvd", "volume", "taker_cov", cfg.min_taker_cov)
    spot = _taker_share(s, w, "spot_cvd", "spot_volume", "spot_taker_cov", cfg.min_taker_cov)
    if perp is None or spot is None:
        return
    t = cfg.spot_perp_taker_share
    facts = {"perp_taker_share": perp, "spot_taker_share": spot, "bars": cfg.window_bars,
             "price_chg_pct": _pct_change(s, "c", w.i0, w.i1)}  # fmt: skip
    if spot >= t and perp <= -t:
        yield Alert("spot_perp_divergence", coin, tf, "spot_buying", s["ts_ms"][w.i1], 3,
                    f"{coin} {tf}: spot buying, perps selling",
                    f"Over the last {cfg.window_bars} {tf} bars spot takers bought "
                    f"({spot * 100:+.0f}% of spot volume) while perp takers sold "
                    f"({perp * 100:+.0f}%).", facts)  # fmt: skip
    elif spot <= -t and perp >= t:
        yield Alert("spot_perp_divergence", coin, tf, "spot_selling", s["ts_ms"][w.i1], 3,
                    f"{coin} {tf}: spot selling, perps buying",
                    f"Over the last {cfg.window_bars} {tf} bars spot takers sold "
                    f"({spot * 100:+.0f}% of spot volume) while perp takers bought "
                    f"({perp * 100:+.0f}%): perp-led move.", facts)  # fmt: skip


def oi_change(payload: Payload, coin: str, tf: str, cfg: AlertsConfig) -> Iterator[Alert]:
    s = _tf_series(payload, coin, tf)
    w = _window(s, 1) if s else None
    need = cfg.oi_change_pct.get(tf)
    if s is None or w is None or need is None:
        return
    oi = _pct_change(s, "oi", w.i0, w.i1)
    price = _pct_change(s, "c", w.i0, w.i1)
    if oi is None or price is None or abs(oi) < need:
        return
    delta = _num((s.get("delta") or [None] * (w.i1 + 1))[w.i1])
    if oi > 0:
        label, direction = ("new longs (long build)", "long_build") if price >= 0 else (
            "new shorts (short build)", "short_build")  # fmt: skip
    else:
        label, direction = ("shorts covering", "covering") if price >= 0 else (
            "longs unwinding", "unwind")  # fmt: skip
    taker = "" if delta is None else f", perp takers net {'buying' if delta > 0 else 'selling'}"
    yield Alert("oi_change", coin, tf, direction, s["ts_ms"][w.i1], 3,
                f"{coin} {tf}: OI {oi:+.1f}%, {label}",
                f"Open interest {oi:+.1f}% and price {price:+.1f}% over the last {tf} bar"
                f"{taker}: looks like {label}.",
                {"oi_chg_pct": oi, "price_chg_pct": price, "bar_delta": delta})  # fmt: skip


def liq_cluster(payload: Payload, coin: str, cfg: AlertsConfig) -> Iterator[Alert]:
    m = payload["coins"][coin].get("liqmap")
    need = cfg.liq_cluster_ntl.get(coin)
    if not m or need is None:
        return
    mark = float(m["mark"])
    d = cfg.liq_cluster_distance_pct / 100
    longs = sum(v for px, v in zip(m["px"], m["long"], strict=True)
                if mark * (1 - d) <= px < mark)  # fmt: skip
    shorts = sum(v for px, v in zip(m["px"], m["short"], strict=True)
                 if mark < px <= mark * (1 + d))  # fmt: skip
    pos = payload["coins"][coin].get("positioning") or {}
    cov = {"long_cov": pos.get("long_cov"), "short_cov": pos.get("short_cov")}
    within = f"within {cfg.liq_cluster_distance_pct:g}% of {mark:,.6g}"
    if longs >= need:
        yield Alert("liq_cluster", coin, "-", "longs_below", None, 4,
                    f"{coin}: {_usd(longs)} of watched longs liquidate nearby",
                    f"{_usd(longs)} of watched long positions liquidate {within} (below). "
                    "A move down there could cascade.",
                    {"mark": mark, "long_ntl": longs, **cov})  # fmt: skip
    if shorts >= need:
        yield Alert("liq_cluster", coin, "-", "shorts_above", None, 4,
                    f"{coin}: {_usd(shorts)} of watched shorts liquidate nearby",
                    f"{_usd(shorts)} of watched short positions liquidate {within} (above). "
                    "A move up there could squeeze.",
                    {"mark": mark, "short_ntl": shorts, **cov})  # fmt: skip


def liquidations(payload: Payload, coin: str, cfg: AlertsConfig) -> Iterator[Alert]:
    s = _tf_series(payload, coin, "1h")
    need = cfg.liquidations_ntl.get(coin)
    closed = _closed(s) if s else []
    if s is None or not closed or need is None:
        return
    i = closed[-1]
    lo, sh = float(s["liq_long"][i]), float(s["liq_short"][i])
    if lo + sh >= need:
        side = "longs" if lo >= sh else "shorts"
        yield Alert("liquidations", coin, "1h", side, s["ts_ms"][i], 4,
                    f"{coin}: {_usd(lo + sh)} liquidated in the last hour",
                    f"Liquidations seen in the {s['ts'][i]} hour: longs {_usd(lo)}, shorts "
                    f"{_usd(sh)} (watched wallets + backstops; the true total is higher).",
                    {"liq_long": lo, "liq_short": sh})  # fmt: skip


def watch_flow(payload: Payload, coin: str, tf: str, cfg: AlertsConfig) -> Iterator[Alert]:
    if tf not in cfg.watch_flow_timeframes:
        return
    s = _tf_series(payload, coin, tf)
    w = _window(s, 1) if s else None
    need = cfg.watch_flow_ntl.get(coin)
    if s is None or w is None or need is None:
        return
    a, b = _num(s["watch_flow"][w.i0]), _num(s["watch_flow"][w.i1])
    if a is None or b is None or abs(b - a) < need:
        return
    change = b - a
    side = "long" if change > 0 else "short"
    yield Alert("watch_flow", coin, tf, f"net_{side}", s["ts_ms"][w.i1], 3,
                f"{coin} {tf}: watchlist {'+' if change > 0 else ''}{_usd(change)} net",
                f"The same watched wallets moved {_usd(change)} net {side} over the last {tf} "
                "bar (big-wallet positioning).",
                {"net_change_usd": change})  # fmt: skip


CoinRule = Callable[[Payload, str, AlertsConfig], Iterator[Alert]]
TfRule = Callable[[Payload, str, str, AlertsConfig], Iterator[Alert]]
COIN_RULES: dict[str, CoinRule] = {
    "premium_extreme": premium_extreme,
    "liq_cluster": liq_cluster,
    "liquidations": liquidations,
}
TF_RULES: dict[str, TfRule] = {
    "cvd_divergence": cvd_divergence,
    "spot_perp_divergence": spot_perp_divergence,
    "oi_change": oi_change,
    "watch_flow": watch_flow,
}
ALL_RULES = (*COIN_RULES, *TF_RULES, "health")


def evaluate(payload: Payload, cfg: AlertsConfig) -> list[Alert]:
    """Every market rule, for every coin (and configured timeframe)."""
    out: list[Alert] = []
    for coin in payload.get("coins", {}):
        for name, coin_rule in COIN_RULES.items():
            if name not in cfg.disabled_rules:
                out.extend(coin_rule(payload, coin, cfg))
        for tf in cfg.timeframes:
            for name, tf_rule in TF_RULES.items():
                if name not in cfg.disabled_rules:
                    out.extend(tf_rule(payload, coin, tf, cfg))
    return out
