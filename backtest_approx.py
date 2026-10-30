#!/usr/bin/env python3
"""
Approximate backtest for Polymarket BTC 5-minute Up/Down markets.
Uses only free public APIs. Conservative fill + fee assumptions.
Run: python backtest_approx.py
"""

import requests
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time
import json
import os
from collections import defaultdict

# ================== CONFIG ==================
DAYS_BACK          = 6          # fine price history ~7 days
FEE_RATE           = 0.07       # Polymarket crypto taker fee: fee = shares × FEE_RATE × p × (1 - p)
SLIPPAGE_CENTS     = 0.015      # 1.5¢ adverse slippage on marketable orders
SIZE_USD           = 20         # fixed size per trade for the backtest
ENABLED_PLAYS      = {"P1_early_lag", "P4_final_extreme"}   # P2/P3 dropped: negative after costs
P1_MAX_ENTRY       = 0.55       # P1 only fills at or below this price (slippage included)
DAILY_LOSS_LIMIT   = 60         # stop opening trades for the UTC day once realized PnL <= -$60
MAX_WORKERS        = 8          # parallel requests; lower this if you start seeing HTTP 429
CACHE_FILE         = "cache_btc5m.json"   # downloaded markets + price history, reused across runs
BINANCE_CACHE_FILE = "cache_binance_1m.json"   # downloaded BTCUSDT 1m candles, reused across runs

GAMMA              = "https://gamma-api.polymarket.com"
DATA_API           = "https://data-api.polymarket.com/v2"
BINANCE            = "https://api.binance.com/api/v3"

# ================== HELPERS ==================
_local = threading.local()

def get_session():
    """One keep-alive session per thread, so connections are reused instead of re-handshaking."""
    s = getattr(_local, "session", None)
    if s is None:
        s = _local.session = requests.Session()
    return s

def get_json(url, params=None, retries=3):
    for i in range(retries):
        try:
            r = get_session().get(url, params=params, timeout=15)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429 or r.status_code >= 500:
                time.sleep(2 + i)
                continue
            print(f"  HTTP {r.status_code} for {url}")
            return None
        except Exception as e:
            print(f"  Request error: {e}")
            time.sleep(1)
    return None

def taker_fee(size_usd, price):
    """Polymarket taker fee in USDC for buying size_usd worth at price (docs.polymarket.com/trading/fees)."""
    shares = size_usd / price
    return round(shares * FEE_RATE * price * (1 - price), 5)

def load_cache(path=CACHE_FILE):
    if not os.path.exists(path):
        return {}
    try:
        with open(path) as f:
            return json.load(f)
    except Exception as e:
        print(f"  Could not read {path} ({e}), starting fresh")
        return {}

def save_cache(cache, path=CACHE_FILE):
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(cache, f)
    os.replace(tmp, path)

def generate_recent_slugs(days_back=DAYS_BACK):
    """Generate closed btc-updown-5m-{unix} slugs for the last N days."""
    now = int(datetime.now(timezone.utc).timestamp())
    # round down to nearest 5-minute boundary
    end = (now // 300) * 300 - 300          # last fully closed window
    start = end - days_back * 86400
    slugs = []
    t = start
    while t <= end:
        slugs.append(f"btc-updown-5m-{t}")
        t += 300
    return slugs

def fetch_market(slug):
    """Fetch one market via Gamma events endpoint."""
    data = get_json(f"{GAMMA}/events", {"slug": slug})
    if not data:
        return None
    if isinstance(data, list):
        if not data:
            return None
        event = data[0]
    else:
        event = data
    markets = event.get("markets") or []
    if not markets:
        return None
    m = markets[0]

    # token ids
    tokens = m.get("clobTokenIds")
    if isinstance(tokens, str):
        try:
            tokens = json.loads(tokens)
        except:
            tokens = []
    if not tokens or len(tokens) < 2:
        return None

    outcomes = m.get("outcomes")
    if isinstance(outcomes, str):
        try:
            outcomes = json.loads(outcomes)
        except:
            outcomes = ["Up", "Down"]

    prices = m.get("outcomePrices")
    if isinstance(prices, str):
        try:
            prices = json.loads(prices)
        except:
            prices = None

    # determine winner
    winner = None
    if prices and len(prices) == 2:
        try:
            p0, p1 = float(prices[0]), float(prices[1])
            if p0 > 0.9:
                winner = outcomes[0]
            elif p1 > 0.9:
                winner = outcomes[1]
        except:
            pass

    start_ts = int(slug.split("-")[-1])
    return {
        "slug": slug,
        "start_ts": start_ts,
        "end_ts": start_ts + 300,
        "condition_id": m.get("conditionId"),
        "up_token": tokens[0],
        "down_token": tokens[1],
        "outcomes": outcomes,
        "winner": winner,
        "volume": float(m.get("volumeNum") or m.get("volume") or 0),
        "closed": m.get("closed", True),
    }

def fetch_price_history(token_id, start_ts, end_ts):
    """Data API v2 – 60-second buckets."""
    params = {
        "token_id": token_id,
        "start": start_ts,
        "end": end_ts + 60,
        "bucket_seconds": 60,
        "limit": 20,
    }
    data = get_json(f"{DATA_API}/prices-history", params)
    if not data or "data" not in data:
        return []
    points = []
    for row in data["data"]:
        points.append({
            "t": int(row["timestamp"]),
            "p": float(row["price"]),
        })
    return sorted(points, key=lambda x: x["t"])

def fetch_window(slug):
    """Market + Up-token history for one window, or None if it isn't resolved / has no history.
    Down is just 1 - Up, so its history isn't fetched."""
    market = fetch_market(slug)
    if not market or not market["winner"]:
        return None
    up_hist = fetch_price_history(market["up_token"], market["start_ts"], market["end_ts"])
    if len(up_hist) < 2:
        return None
    return {"market": market, "up_hist": up_hist}

def fetch_binance_range(start_ts, end_ts):
    """All BTCUSDT 1-minute klines in [start_ts, end_ts), 1000 per request. Returns {open_ts: candle}."""
    candles = {}
    cur = start_ts
    while cur < end_ts:
        params = {
            "symbol": "BTCUSDT",
            "interval": "1m",
            "startTime": cur * 1000,
            "endTime": end_ts * 1000 - 1,
            "limit": 1000,
        }
        data = get_json(f"{BINANCE}/klines", params)
        if not data:
            break
        for k in data:
            t = int(k[0]) // 1000
            candles[t] = {
                "t": t,
                "open": float(k[1]),
                "high": float(k[2]),
                "low": float(k[3]),
                "close": float(k[4]),
            }
        cur = int(data[-1][0]) // 1000 + 60
    return candles

def fetch_binance_cached(start_ts, end_ts):
    """Like fetch_binance_range, but only downloads minutes missing from BINANCE_CACHE_FILE."""
    candles = {int(t): c for t, c in load_cache(BINANCE_CACHE_FILE).items()}
    missing = [t for t in range(start_ts, end_ts, 60) if t not in candles]
    if missing:
        candles.update(fetch_binance_range(missing[0], end_ts))
        save_cache(candles, BINANCE_CACHE_FILE)
    return candles

# ================== PLAYS (transparent & conservative) ==================
def simulate_plays(market, up_hist, btc_candles):
    """
    Four simple plays. All use conservative fill assumptions.
    btc_candles: the window's five 1-minute candles, first one opening at start_ts.
    Returns list of trade dicts (or empty).
    """
    trades = []
    if not market["winner"] or not up_hist or not btc_candles:
        return trades

    start = market["start_ts"]
    winner = market["winner"]  # "Up" or "Down"

    # helper: price of Up as of a given second. Buckets are point-in-time prices stamped on the
    # minute, so only buckets at or before ts are used (never peek at a later one).
    def price_at(hist, ts):
        before = [x for x in hist if x["t"] <= ts]
        if not before:
            return None
        best = before[-1]
        if ts - best["t"] > 90:
            return None
        return best["p"]

    # ---------- Play 1: Minute-0 lag catch ----------
    # At the 1-minute mark, if Binance already moved >0.15% during minute 0 and Polymarket is still
    # near 0.50. Uses the minute-0 candle (closes at start+60) and the Up price at start+60, so nothing
    # after the decision is known. Data is 1-minute resolution, so this can't be judged before 60s.
    t1 = start + 60
    up_p = price_at(up_hist, t1)
    m0 = btc_candles[0]
    btc_move = (m0["close"] - m0["open"]) / m0["open"]
    if up_p is not None and abs(btc_move) > 0.0015:
        side = "Up" if btc_move > 0 else "Down"
        entry = up_p if side == "Up" else (1 - up_p)
        # only take if still reasonably close to 0.50
        if 0.42 < entry < 0.58:
            fill = entry + SLIPPAGE_CENTS
            if fill <= P1_MAX_ENTRY:
                trades.append({
                    "play": "P1_early_lag",
                    "side": side,
                    "entry": round(fill, 3),
                    "size_usd": SIZE_USD,
                    "won": side == winner,
                    "t": t1,
                })

    # ---------- Play 2: Minute 1-2 continuation ----------
    t2 = start + 90
    up_p = price_at(up_hist, t2)
    if up_p is not None:
        # if price has already moved meaningfully
        if up_p > 0.62 or up_p < 0.38:
            side = "Up" if up_p > 0.5 else "Down"
            entry = up_p if side == "Up" else (1 - up_p)
            fill = entry + SLIPPAGE_CENTS
            if fill < 0.92:
                trades.append({
                    "play": "P2_continuation",
                    "side": side,
                    "entry": round(fill, 3),
                    "size_usd": SIZE_USD,
                    "won": side == winner,
                    "t": t2,
                })

    # ---------- Play 3: Minute 3 panic / confirmation ----------
    t3 = start + 180
    up_p = price_at(up_hist, t3)
    if up_p is not None:
        if up_p > 0.72 or up_p < 0.28:
            side = "Up" if up_p > 0.5 else "Down"
            entry = up_p if side == "Up" else (1 - up_p)
            fill = entry + SLIPPAGE_CENTS
            if fill < 0.90:
                trades.append({
                    "play": "P3_late_confirm",
                    "side": side,
                    "entry": round(fill, 3),
                    "size_usd": SIZE_USD,
                    "won": side == winner,
                    "t": t3,
                })

    # ---------- Play 4: Final 60s only if extreme ----------
    t4 = start + 240
    up_p = price_at(up_hist, t4)
    if up_p is not None:
        if up_p > 0.85 or up_p < 0.15:
            side = "Up" if up_p > 0.5 else "Down"
            entry = up_p if side == "Up" else (1 - up_p)
            fill = entry + SLIPPAGE_CENTS * 0.5   # less slippage late
            if fill < 0.97:
                trades.append({
                    "play": "P4_final_extreme",
                    "side": side,
                    "entry": round(fill, 3),
                    "size_usd": SIZE_USD,
                    "won": side == winner,
                    "t": t4,
                })

    return trades

def apply_daily_loss_limit(df):
    """Skip trades opened after the day's realized PnL reached -DAILY_LOSS_LIMIT.
    A trade's PnL only counts as realized once its window closes (start_ts + 300)."""
    df = df.sort_values("t").reset_index(drop=True)
    keep = []
    taken = []   # (date, close_ts, pnl) of trades taken so far
    for row in df.itertuples():
        realized = sum(p for d, close, p in taken if d == row.date and close <= row.t)
        if realized <= -DAILY_LOSS_LIMIT:
            keep.append(False)
            continue
        keep.append(True)
        taken.append((row.date, row.start_ts + 300, row.pnl))
    return df[keep]

# ================== MAIN ==================
def main():
    print("=" * 60)
    print("Polymarket BTC 5m – Approximate Backtest")
    print(f"Looking back ~{DAYS_BACK} days (fine history window)")
    print("=" * 60)

    slugs = generate_recent_slugs()
    print(f"Generated {len(slugs)} candidate windows")

    t0 = time.time()
    first_start = int(slugs[0].split("-")[-1])
    last_end = int(slugs[-1].split("-")[-1]) + 300
    binance = fetch_binance_cached(first_start, last_end)
    print(f"Binance: {len(binance)} 1m candles in {time.time() - t0:.0f}s")

    cache = load_cache()
    todo = [s for s in slugs if s not in cache]
    print(f"Cached windows: {len(slugs) - len(todo)}, to download: {len(todo)}")

    # Only resolved windows with usable history are cached; the rest are retried next run.
    t0 = time.time()
    unsaved = 0
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
                cache[slug] = entry
                unsaved += 1
            if unsaved >= 200:
                save_cache(cache)
                unsaved = 0
            if i % 100 == 0:
                print(f"  Progress: {i}/{len(todo)}  ({time.time() - t0:.0f}s)")
    save_cache(cache)
    print(f"Downloaded in {time.time() - t0:.0f}s")

    all_trades = []
    markets_ok = 0
    markets_skipped = 0

    for slug in slugs:
        entry = cache.get(slug)
        if not entry:
            markets_skipped += 1
            continue
        market, up_hist = entry["market"], entry["up_hist"]
        start = market["start_ts"]
        btc = [binance[t] for t in range(start, start + 300, 60) if t in binance]

        if len(btc) < 5 or btc[0]["t"] != start:
            markets_skipped += 1
            continue

        markets_ok += 1
        trades = [t for t in simulate_plays(market, up_hist, btc) if t["play"] in ENABLED_PLAYS]
        for t in trades:
            t["slug"] = slug
            t["winner"] = market["winner"]
            t["start_ts"] = market["start_ts"]
            all_trades.append(t)

    print(f"\nMarkets with usable data: {markets_ok}")
    print(f"Skipped: {markets_skipped}")
    print(f"Total simulated trades: {len(all_trades)}")

    if not all_trades:
        print("No trades generated. Try increasing DAYS_BACK or check connectivity.")
        return

    df = pd.DataFrame(all_trades)

    # P&L calculation (conservative)
    def pnl(row):
        # $SIZE_USD buys SIZE_USD / entry shares, each paying $1 if that side wins
        payout = SIZE_USD / row["entry"] if row["won"] else 0.0
        return payout - SIZE_USD - taker_fee(SIZE_USD, row["entry"])

    df["pnl"] = df.apply(pnl, axis=1)
    df["date"] = pd.to_datetime(df["start_ts"], unit="s", utc=True).dt.date

    n_before = len(df)
    df = apply_daily_loss_limit(df)
    print(f"Daily loss limit (${DAILY_LOSS_LIMIT}) blocked {n_before - len(df)} trades")

    # ===== REPORT =====
    print("\n" + "=" * 60)
    print("RESULTS BY PLAY (after fees + slippage)")
    print("=" * 60)

    for play, g in df.groupby("play"):
        n = len(g)
        wins = g["won"].sum()
        wr = wins / n * 100
        avg_entry = g["entry"].mean()
        total_pnl = g["pnl"].sum()
        avg_pnl = g["pnl"].mean()
        worst_day = g.groupby("date")["pnl"].sum().min()
        print(f"\n{play}")
        print(f"  Trades     : {n}")
        print(f"  Win rate   : {wr:.1f}%")
        print(f"  Avg entry  : {avg_entry:.3f}")
        print(f"  Total PnL  : ${total_pnl:+.2f}")
        print(f"  Avg PnL    : ${avg_pnl:+.2f}")
        print(f"  Worst day  : ${worst_day:+.2f}")

    print("\n" + "=" * 60)
    print("OVERALL")
    print("=" * 60)
    print(f"Total trades : {len(df)}")
    print(f"Win rate     : {df['won'].mean()*100:.1f}%")
    print(f"Net PnL      : ${df['pnl'].sum():+.2f}")
    print(f"Avg per trade: ${df['pnl'].mean():+.2f}")

    # save
    df.to_csv("trades_approx.csv", index=False)
    print("\nSaved detailed trades → trades_approx.csv")
    print("Review the CSV, then we move to paper trading (B).")

if __name__ == "__main__":
    main()
