#!/usr/bin/env python3
"""
Second-level lag test for Polymarket BTC 5-minute Up/Down markets.
Question: when BTC moves on Binance early in a window, does Polymarket reprice a few seconds
later, and is buying in that gap profitable after costs?
Data: Polymarket taker trades (per-second timestamps) + Binance 1-second candles.
Run: python lag_test.py   (downloads once into cache_seconds/, then only analyses)
"""

import os
import time
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

import backtest_approx as bt

# ================== CONFIG ==================
DAYS_BACK        = 6
MAX_WORKERS      = 6          # parallel windows; lower this if you start seeing HTTP 429 / timeouts
CACHE_DIR        = "cache_seconds"   # one JSON file per window
ENTRY_END        = 120        # only look for signals in the first 2 minutes of a window
HORIZON          = 180        # seconds of data kept per window (signal period + follow-up)
MIN_SIZE         = 5          # ignore trades smaller than this many shares (odd-price dust prints)
MAX_STALE        = 10         # a "last price" older than this many seconds counts as unknown
SETTLE_DELAY     = 2          # data-api trade timestamps are Polygon block times; measured live ~2s
                              # (1.2–2.8s) after the actual match, so trades are shifted back by this
FILL_WINDOW      = 3          # fill = first trade buying our side, big enough for our whole order,
                              # within this many seconds of the signal
EXTRA_SLIPPAGE   = 0.005      # added on top of that trade's price
MOVE_THRESHOLDS  = [0.0003, 0.0005, 0.0010, 0.0015]   # BTC move since window open that triggers a signal
MAX_FILLS        = [0.55, 0.60, 0.65, 0.70]            # only buy if the fill is at or below this
OFFSETS          = [-10, -5, 0, 1, 2, 3, 5, 10, 20, 30]

TRADES_URL       = "https://data-api.polymarket.com/trades"

# ================== DOWNLOAD ==================
def cache_path(slug):
    return os.path.join(CACHE_DIR, f"{slug}.json")

def fetch_trades(condition_id, start_ts):
    """All taker trades from start_ts-60 to start_ts+HORIZON, or None if a page failed."""
    rows, offset = [], 0
    while True:
        page = bt.get_json(TRADES_URL, {"market": condition_id, "start": start_ts - 60,
                                        "end": start_ts + HORIZON, "limit": 1000, "offset": offset})
        if page is None:
            return None
        rows += page
        if len(page) < 1000:
            return rows
        offset += 1000

def fetch_btc_1s(start_ts):
    """Binance 1-second candles for the window as [ts, open, close]."""
    data = bt.get_json(f"{bt.BINANCE}/klines", {"symbol": "BTCUSDT", "interval": "1s",
                                                "startTime": start_ts * 1000, "limit": HORIZON})
    return [[int(k[0]) // 1000, float(k[1]), float(k[4])] for k in data] if data else None

def fetch_window(slug):
    market = bt.fetch_market(slug)
    if not market or not market["winner"] or not market.get("condition_id"):
        return None
    start = market["start_ts"]
    trades = fetch_trades(market["condition_id"], start)
    btc = fetch_btc_1s(start)
    if trades is None or not btc:
        return None
    return {
        "start": start,
        "winner": market["winner"],
        # [ts, token U/D, taker side B/S, price, size]
        "trades": sorted([t["timestamp"], "U" if t["asset"] == market["up_token"] else "D",
                          t["side"][0], float(t["price"]), float(t["size"])] for t in trades),
        "btc": btc,
    }

def download(slugs):
    os.makedirs(CACHE_DIR, exist_ok=True)
    todo = [s for s in slugs if not os.path.exists(cache_path(s))]
    print(f"Cached windows: {len(slugs) - len(todo)}, to download: {len(todo)}")
    t0 = time.time()
    failed = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(fetch_window, s): s for s in todo}
        for i, fut in enumerate(as_completed(futures), 1):
            slug = futures[fut]
            try:
                entry = fut.result()
            except Exception as e:
                print(f"  {slug}: {e}")
                entry = None
            if entry:
                bt.save_cache(entry, cache_path(slug))
            else:
                failed += 1
            if i % 50 == 0:
                print(f"  Progress: {i}/{len(todo)}  failed {failed}  ({time.time() - t0:.0f}s)")
    print(f"Downloaded in {time.time() - t0:.0f}s, {failed} windows failed (rerun to retry them)")

# ================== ANALYSIS ==================
def window_view(w):
    """Per-second view of one window; seconds are relative to the window start.
    ret[s]       BTC return since open at the close of second s (known from second s+1 on)
    up_known[T]  last Up price from trades matched <= T-1, i.e. what you knew at second T
    buys[side]   (second, price, size) of trades that bought that side, in that side's price terms
    Trade seconds are match times: block timestamp - SETTLE_DELAY."""
    start = w["start"]
    btc = {t - start: (o, c) for t, o, c in w["btc"]}
    if 0 not in btc:
        return None
    open0 = btc[0][0]
    ret = {s: c / open0 - 1 for s, (o, c) in btc.items()}

    t0 = start + SETTLE_DELAY
    trades = [t for t in w["trades"] if t[4] >= MIN_SIZE]
    up_known, last_p, last_t, i = {}, None, None, 0
    for T in range(-59, HORIZON + 1):
        while i < len(trades) and trades[i][0] - t0 <= T - 1:
            ts, tok, side, p, size = trades[i]
            last_p, last_t = (p if tok == "U" else 1 - p), ts - t0
            i += 1
        up_known[T] = last_p if last_p is not None and T - last_t <= MAX_STALE else None

    buys = {"Up": [], "Down": []}
    for ts, tok, side, p, size in trades:
        if (tok, side) in (("U", "B"), ("D", "S")):
            buys["Up"].append((ts - t0, p if tok == "U" else 1 - p, size))
        else:
            buys["Down"].append((ts - t0, p if tok == "D" else 1 - p, size))
    return ret, up_known, buys

def first_cross(ret, threshold):
    """First second T (signal time) at which BTC's move since open is >= threshold."""
    for s in range(ENTRY_END):
        r = ret.get(s)
        if r is not None and abs(r) >= threshold:
            return s + 1, ("Up" if r > 0 else "Down")
    return None, None

def first_fill(side_buys, T):
    for sec, p, size in side_buys:
        if sec < T:
            continue
        if sec > T + FILL_WINDOW:
            return None
        if size >= bt.SIZE_USD / p:
            return p + EXTRA_SLIPPAGE
    return None

def spread_check(windows):
    """Median (price of Up-buying trades - price of Up-selling trades) within the same second.
    Positive means taker sides are read correctly (buyers pay the ask, sellers hit the bid)."""
    diffs = []
    for w in windows:
        by_sec = {}
        for ts, tok, side, p, size in w["trades"]:
            if size < MIN_SIZE:
                continue
            up_p = p if tok == "U" else 1 - p
            buying_up = (tok, side) in (("U", "B"), ("D", "S"))
            by_sec.setdefault(ts, ([], []))[0 if buying_up else 1].append(up_p)
        for b, s in by_sec.values():
            if b and s:
                diffs.append(sum(b) / len(b) - sum(s) / len(s))
    return pd.Series(diffs).median() if diffs else None

def analyse(windows):
    starts = sorted(w["start"] for w in windows)
    split = starts[len(starts) // 2]
    events = []
    for w in windows:
        view = window_view(w)
        if view is None:
            continue
        ret, up_known, buys = view
        for th in MOVE_THRESHOLDS:
            T, side = first_cross(ret, th)
            if T is None:
                continue
            def side_price(t):
                up = up_known.get(t)
                return None if up is None else (up if side == "Up" else 1 - up)
            ev = {"start": w["start"], "half": "first" if w["start"] < split else "second",
                  "threshold": th, "T": T, "side": side, "won": side == w["winner"],
                  "at_open": side_price(0), "fill": first_fill(buys[side], T)}
            for k in OFFSETS:
                ev[f"T{k:+d}"] = side_price(T + k)
            events.append(ev)
    ev = pd.DataFrame(events)

    print("\n" + "=" * 78)
    print("EVENT STUDY: median price of the side BTC moved toward, around the signal")
    print("(T = first second the BTC move since open crossed the threshold)")
    print("=" * 78)
    cols = ["at_open"] + [f"T{k:+d}" for k in OFFSETS]
    rows = []
    for th, g in ev.groupby("threshold"):
        row = {"move": f"{th*100:.2f}%", "events": len(g), "median T": int(g["T"].median())}
        row.update({c: round(g[c].median(), 3) for c in cols})
        row["side won"] = f"{g['won'].mean()*100:.0f}%"
        rows.append(row)
    print(pd.DataFrame(rows).to_string(index=False))

    print("\n" + "=" * 78)
    print(f"TRADES: buy that side at the first full-size trade within {FILL_WINDOW}s of the signal "
          f"+ {EXTRA_SLIPPAGE*100:.1f}c, ${bt.SIZE_USD}/trade, Polymarket taker fee")
    print(f"(trade times = block time - {SETTLE_DELAY}s settlement delay)")
    print("first half / second half of the days are reported separately: a rule only counts")
    print("if it makes money in both")
    print("=" * 78)
    rows = []
    for th in MOVE_THRESHOLDS:
        for cap in MAX_FILLS:
            row = {"move": f"{th*100:.2f}%", "max fill": cap}
            for half in ("first", "second"):
                g = ev[(ev["threshold"] == th) & (ev["half"] == half) & ev["fill"].notna() & (ev["fill"] <= cap)]
                fees = g["fill"].apply(lambda p: bt.taker_fee(bt.SIZE_USD, p))
                pnl = (bt.SIZE_USD / g["fill"]).where(g["won"], 0.0) - bt.SIZE_USD - fees
                row[f"{half}: trades"] = len(g)
                row[f"{half}: win%"] = round(g["won"].mean() * 100, 1) if len(g) else None
                row[f"{half}: avg fill"] = round(g["fill"].mean(), 3) if len(g) else None
                row[f"{half}: PnL"] = round(pnl.sum(), 2)
            rows.append(row)
    print(pd.DataFrame(rows).to_string(index=False))

    ev.to_csv("lag_events.csv", index=False)
    print("\nSaved every signal → lag_events.csv")

# ================== MAIN ==================
def main():
    print("=" * 78)
    print("Polymarket BTC 5m – second-level lag test")
    print("=" * 78)
    slugs = bt.generate_recent_slugs(DAYS_BACK)
    print(f"Generated {len(slugs)} candidate windows")
    download(slugs)

    windows = [bt.load_cache(cache_path(s)) for s in slugs if os.path.exists(cache_path(s))]
    windows = [w for w in windows if w]
    print(f"\nWindows with data: {len(windows)}")
    if not windows:
        return
    n_trades = pd.Series([len(w["trades"]) for w in windows])
    print(f"Taker trades per window (first {HORIZON}s): median {int(n_trades.median())}")
    sc = spread_check(windows)
    if sc is not None:
        print(f"Spread check (Up buyers minus Up sellers, same second): median {sc:+.3f}"
              + ("" if sc >= 0 else "  <-- negative: taker side may be read wrong"))
    analyse(windows)

if __name__ == "__main__":
    main()
