#!/usr/bin/env python3
"""
Paper-trading bot for the Polymarket BTC 5-minute lag idea. It never sends a real order.
Watches Binance BTCUSDT 1-second candles and the Polymarket order book live. In the first
ENTRY_END seconds of each window, the first time a candle closes MOVE_THRESHOLD away from the
window's open, it works out what a SIZE_USD market buy of that side would have filled at from the
live order book (at the signal and DELAYS seconds later), then logs the result once the market
resolves. Signals use the same rule as lag_test.py.
Run: python paper_bot.py               (runs until Ctrl+C)
     python paper_bot.py --minutes 60  (stops after an hour, then waits for pending results)
Results append to paper_trades.csv.
"""

import argparse
import csv
import json
import os
import threading
import time
from datetime import datetime, timezone

import requests
import websocket

import backtest_approx as bt

# ================== CONFIG ==================
MOVE_THRESHOLD = 0.0003         # BTC move since window open that triggers a signal (0.03%)
ENTRY_END      = 120            # only signal in the first 2 minutes of a window
MAX_FILL       = 0.70           # paper-buy only if the average fill is at or below this
SIZE_USD       = bt.SIZE_USD    # dollars per paper trade
DELAYS         = [0, 1, 2, 3]   # also price the same order this many seconds after the signal
MAX_BOOK_AGE   = 2              # seconds; order-book data older than this (by exchange time) is not counted
LOG_FILE       = "paper_trades.csv"

BINANCE_WS     = "wss://stream.binance.com:9443/ws/btcusdt@kline_1s"
POLY_WS        = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

# ================== HELPERS ==================
def log(msg):
    print(f"[{datetime.now(timezone.utc):%H:%M:%S}] {msg}", flush=True)

def utc(ts):
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def rnd(x, n=3):
    return "" if x is None else round(x, n)

class Clock:
    """Local clock corrected to Binance server time (window boundaries are exchange time)."""
    def __init__(self):
        self.offset = 0.0

    def sync(self):
        try:
            t0 = time.time()
            srv = requests.get(f"{bt.BINANCE}/time", timeout=10).json()["serverTime"] / 1000
            self.offset = srv - (t0 + time.time()) / 2
        except Exception as e:
            log(f"Clock sync failed ({e}), keeping offset {self.offset:+.2f}s")

    def now(self):
        return time.time() + self.offset

clock = Clock()

def sleep_until(t):
    while (left := t - clock.now()) > 0:
        time.sleep(min(left, 1))

# ================== FEEDS ==================
class BinanceFeed:
    """Keeps the BTCUSDT 1-second candle stream open; calls on_candle(open_ts, open, close) per closed candle."""
    def __init__(self, on_candle):
        self.on_candle = on_candle
        self.last_lag = None    # seconds between a candle closing and us receiving it
        threading.Thread(target=self._run, daemon=True).start()

    def _run(self):
        while True:
            ws = websocket.WebSocketApp(BINANCE_WS, on_message=self._on_message,
                                        on_error=lambda w, e: log(f"Binance feed error: {e}"))
            ws.run_forever(ping_interval=20, ping_timeout=10)
            log("Binance feed disconnected, reconnecting in 2s")
            time.sleep(2)

    def _on_message(self, ws, msg):
        k = json.loads(msg)["k"]
        if k["x"]:
            t = k["t"] / 1000
            self.last_lag = clock.now() - (t + 1)
            self.on_candle(t, float(k["o"]), float(k["c"]))

class PolyBook:
    """Live order book of one window's Up token. The Down book is its mirror image (a Down ask at q
    is an Up bid at 1 - q, same size), so subscribing to Up alone covers both sides at half the bandwidth."""
    def __init__(self, up_token):
        self.token = up_token
        self.bids, self.asks = {}, {}
        self.server_ts = None    # exchange timestamp of the last message applied
        self.lock = threading.Lock()
        self.running = True
        self.ws = None
        threading.Thread(target=self._run, daemon=True).start()

    def _run(self):
        while self.running:
            self.ws = websocket.WebSocketApp(POLY_WS, on_open=self._on_open, on_message=self._on_message,
                                             on_error=lambda w, e: log(f"Polymarket feed error: {e}"))
            self.ws.run_forever()
            if self.running:
                log("Polymarket feed disconnected, reconnecting in 1s")
                time.sleep(1)

    def _on_open(self, ws):
        ws.send(json.dumps({"assets_ids": [self.token], "type": "market"}))
        def ping():
            while ws.keep_running:
                time.sleep(10)
                try:
                    ws.send("PING")
                except Exception:
                    return
        threading.Thread(target=ping, daemon=True).start()

    def _on_message(self, ws, msg):
        if msg == "PONG":
            return
        data = json.loads(msg)
        with self.lock:
            for ev in (data if isinstance(data, list) else [data]):
                et = ev.get("event_type")
                if et == "book" and ev.get("asset_id") == self.token:
                    self.bids = {float(l["price"]): float(l["size"]) for l in ev["bids"]}
                    self.asks = {float(l["price"]): float(l["size"]) for l in ev["asks"]}
                    self.server_ts = int(ev["timestamp"]) / 1000
                elif et == "price_change":
                    for ch in ev.get("price_changes", []):
                        if ch["asset_id"] != self.token:
                            continue
                        levels = self.bids if ch["side"] == "BUY" else self.asks
                        p, q = float(ch["price"]), float(ch["size"])
                        if q == 0:
                            levels.pop(p, None)
                        else:
                            levels[p] = q
                        self.server_ts = int(ev["timestamp"]) / 1000

    def market_buy(self, side, usd):
        """What a usd market buy of side would fill at right now: walks that side's asks, adds the taker fee.
        age = how old the book is in exchange time (a lagging feed shows up here)."""
        with self.lock:
            if side == "Up":
                levels = sorted(self.asks.items())
                best_bid = max(self.bids) if self.bids else None
            else:
                levels = sorted((round(1 - p, 4), q) for p, q in self.bids.items())
                best_bid = round(1 - min(self.asks), 4) if self.asks else None
            age = clock.now() - self.server_ts if self.server_ts else None
        spent = shares = fee = 0.0
        for p, q in levels:
            take = min(q, (usd - spent) / p)
            shares += take
            spent += take * p
            fee += take * bt.FEE_RATE * p * (1 - p)
            if spent >= usd - 1e-9:
                break
        return {"avg": spent / shares if shares else None, "shares": shares, "spent": spent, "fee": fee,
                "best_ask": levels[0][0] if levels else None, "best_bid": best_bid, "age": age}

    def stop(self):
        self.running = False
        if self.ws:
            self.ws.close()

# ================== BOT ==================
def tradable(fill):
    return (fill["avg"] is not None and fill["spent"] >= SIZE_USD - 0.01 and fill["avg"] <= MAX_FILL
            and fill["age"] is not None and fill["age"] <= MAX_BOOK_AGE)

class Window:
    def __init__(self, market):
        self.slug = market["slug"]
        self.start = market["start_ts"]
        self.open = None      # BTC price at the window start; False if that candle was missed
        self.signal = None
        self.book = PolyBook(market["up_token"])

class PaperBot:
    def __init__(self):
        self.window = None
        self.csv_lock = threading.Lock()
        self.resolvers = []
        self.results = []
        if os.path.exists(LOG_FILE):
            with open(LOG_FILE, newline="") as f:
                self.results = list(csv.DictReader(f))
            log(f"Loaded {len(self.results)} earlier signals from {LOG_FILE}")
        self.feed = BinanceFeed(self.on_candle)

    def on_candle(self, t, o, c):
        w = self.window
        if w is None or t < w.start:
            return
        if w.open is None:
            if t == w.start:
                w.open = o
                log(f"{w.slug}: BTC open {o:.2f} (Binance feed lag {self.feed.last_lag:.2f}s)")
            else:
                w.open = False
                log(f"{w.slug}: missed the first BTC candle of the window, skipping")
                return
        if w.open is False or w.signal is not None or t >= w.start + ENTRY_END:
            return
        move = c / w.open - 1
        if abs(move) >= MOVE_THRESHOLD:
            self.on_signal(w, c, t + 1, move)

    def on_signal(self, w, price, ts, move):
        side = "Up" if move > 0 else "Down"
        sig = {"slug": w.slug, "signal_utc": utc(ts), "sec_into_window": round(ts - w.start),
               "btc_open": w.open, "btc_price": price, "move_pct": round(move * 100, 4), "side": side,
               "btc_feed_lag": rnd(self.feed.last_lag, 2), "fills": {}}
        w.signal = sig

        def snap(d):
            sig["fills"][d] = w.book.market_buy(side, SIZE_USD)
            if d == max(DELAYS):
                self.report_signal(w, sig)

        snap(0)
        for d in DELAYS[1:]:
            threading.Timer(d, snap, args=(d,)).start()

    def report_signal(self, w, sig):
        f0 = sig["fills"][0]
        if f0["age"] is None or f0["age"] > MAX_BOOK_AGE:
            sig["status"] = "stale book"
        elif f0["spent"] < SIZE_USD - 0.01:
            sig["status"] = "book too thin"
        elif f0["avg"] > MAX_FILL:
            sig["status"] = f"fill above {MAX_FILL}"
        else:
            sig["status"] = "paper buy"
        fills = " ".join(f"+{d}s {sig['fills'][d]['avg']:.3f}" for d in DELAYS if sig["fills"][d]["avg"])
        age = f"{f0['age']:.1f}s" if f0["age"] is not None else "none"
        log(f"{w.slug}: BTC {sig['move_pct']:+.3f}% at +{sig['sec_into_window']}s -> {sig['side']} | "
            f"bid/ask {f0['best_bid']}/{f0['best_ask']} (book age {age}) | avg fill {fills} | {sig['status'].upper()}")

    def resolve_later(self, w):
        def run():
            sleep_until(w.start + 300 + 20)
            deadline = w.start + 300 + 30 * 60
            while clock.now() < deadline:
                m = bt.fetch_market(w.slug)
                if m and m["winner"]:
                    self.record(w, m["winner"])
                    return
                time.sleep(30)
            log(f"{w.slug}: no result after 30 min, not recorded")
        t = threading.Thread(target=run, daemon=True)
        t.start()
        self.resolvers.append(t)

    def record(self, w, winner):
        sig = w.signal
        won = sig["side"] == winner
        f0 = sig["fills"][0]
        row = {k: sig[k] for k in ("slug", "signal_utc", "sec_into_window", "btc_open", "btc_price",
                                   "move_pct", "side", "btc_feed_lag", "status")}
        row.update({"book_age": rnd(f0["age"], 2), "best_bid": f0["best_bid"], "best_ask": f0["best_ask"],
                    "winner": winner, "won": won})
        for d in DELAYS:
            f = sig["fills"][d]
            row[f"fill_{d}s"] = rnd(f["avg"], 4)
            row[f"pnl_{d}s"] = rnd((f["shares"] if won else 0.0) - f["spent"] - f["fee"], 2) if tradable(f) else ""
        with self.csv_lock:
            new = not os.path.exists(LOG_FILE)
            with open(LOG_FILE, "a", newline="") as fh:
                wr = csv.DictWriter(fh, fieldnames=list(row))
                if new:
                    wr.writeheader()
                wr.writerow(row)
            self.results.append(row)
        result = f"${row['pnl_0s']:+.2f}" if row["pnl_0s"] != "" else f"not traded ({sig['status']})"
        log(f"{w.slug}: {winner} won, {sig['side']} {'WIN' if won else 'LOSS'} -> {result}")
        log("TOTALS " + self.summary())

    def summary(self):
        parts = []
        for d in DELAYS:
            pnls = [float(r[f"pnl_{d}s"]) for r in self.results if r.get(f"pnl_{d}s") not in ("", None)]
            if d == 0:
                wins = sum(p > 0 for p in pnls)
                parts.append(f"{len(self.results)} signals, {len(pnls)} paper trades, "
                             f"{(wins / len(pnls) * 100) if pnls else 0:.0f}% win, PnL {sum(pnls):+.2f}")
            else:
                parts.append(f"+{d}s late {sum(pnls):+.2f} ({len(pnls)})")
        return " | ".join(parts)

# ================== MAIN ==================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--minutes", type=float, default=0, help="stop after this many minutes (0 = run until Ctrl+C)")
    args = ap.parse_args()
    stop_at = time.time() + args.minutes * 60 if args.minutes else None

    log("Paper bot started. No real orders are ever sent. Ctrl+C to stop.")
    log(f"Signal: BTC {MOVE_THRESHOLD*100:.2f}% from open within {ENTRY_END}s | ${SIZE_USD}/trade | "
        f"max fill {MAX_FILL} | book must be <= {MAX_BOOK_AGE}s old | taker fee {bt.FEE_RATE} x p x (1-p) per share")
    clock.sync()
    bot = PaperBot()
    try:
        while stop_at is None or time.time() < stop_at:
            clock.sync()
            start = (int(clock.now()) // 300 + 1) * 300
            slug = f"btc-updown-5m-{start}"
            log(f"Next window {slug} starts in {start - clock.now():.0f}s (clock offset {clock.offset:+.2f}s)")
            sleep_until(start - 20)
            market = None
            while market is None and clock.now() < start + 30:
                market = bt.fetch_market(slug)
                if market is None:
                    time.sleep(3)
            if market is None:
                log(f"{slug}: market not found, skipping")
                continue
            w = Window(market)
            bot.window = w
            sleep_until(start + ENTRY_END + max(DELAYS) + 2)
            bot.window = None
            w.book.stop()
            if w.signal is None:
                if w.open:
                    log(f"{slug}: no signal (BTC stayed within {MOVE_THRESHOLD*100:.2f}% for {ENTRY_END}s)")
            else:
                bot.resolve_later(w)
        log("Time limit reached, waiting for pending results...")
        for t in bot.resolvers:
            t.join(timeout=600)
    except KeyboardInterrupt:
        log("Stopped.")
    log("TOTALS " + bot.summary())

if __name__ == "__main__":
    main()
