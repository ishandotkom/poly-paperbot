#!/usr/bin/env python3
"""
Paper-trading bot for the Polymarket BTC 5-minute lag idea. It never sends a real order.
Watches Binance BTCUSDT and the Polymarket order book live. In the first ENTRY_END seconds of each
window, the first time BTC is MOVE_THRESHOLD away from the window's open, it works out what a
SIZE_USD market buy of that side would have filled at from the live order book (at the signal and
DELAYS seconds later), then logs the result once the market resolves.
Two signal modes run side by side on the same windows, each with its own results file:
  candle    a 1-second candle closes beyond the threshold (the same rule as lag_test.py)
  realtime  the first trade beyond the threshold, up to about a second sooner
Run: python paper_bot.py               (runs until Ctrl+C)
     python paper_bot.py --minutes 60  (stops after an hour, then waits for pending results)
     python paper_bot.py --port 8000   (also serves the dashboard at http://localhost:8000)
Results append to the LOG_FILES. On Render the dashboard is served on $PORT automatically.
"""

import argparse
import csv
import json
import os
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

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
LOG_FILES      = {"candle": "paper_trades.csv",              # signal when a 1-second candle closes beyond the threshold
                  "realtime": "paper_trades_realtime.csv"}  # signal on the first trade beyond it
KEEP_ALIVE_MIN = 10             # on Render, ping our own public URL this often so a free instance doesn't sleep

BINANCE_WS     = "wss://stream.binance.com:9443/stream?streams=btcusdt@kline_1s/btcusdt@aggTrade"
POLY_WS        = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
GIST_API       = "https://api.github.com/gists"

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

class GistStore:
    """Keeps the results files in a secret GitHub Gist, because a free Render instance loses its files on
    every restart or deploy. Turned on by the env vars GITHUB_TOKEN (a token with the gist scope) and GIST_ID."""
    def __init__(self):
        self.gist_id = os.environ.get("GIST_ID")
        token = os.environ.get("GITHUB_TOKEN")
        self.enabled = bool(self.gist_id and token)
        self.headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}
        self.lock = threading.Lock()

    def restore(self, paths):
        """Downloads each results file that is missing locally. If the Gist can't be read, saving is turned
        off for this run, so a fresh empty file never overwrites the results saved there."""
        if not self.enabled:
            log("Gist sync off (set GITHUB_TOKEN and GIST_ID to keep results across restarts)")
            return
        for attempt in range(5):
            try:
                r = requests.get(f"{GIST_API}/{self.gist_id}", headers=self.headers, timeout=30)
                r.raise_for_status()
                files, texts = r.json()["files"], {}
                for path in paths:
                    f = files.get(path)
                    if f and not os.path.exists(path):
                        # the API cuts file contents off at 1 MB; raw_url has the whole file
                        texts[path] = requests.get(f["raw_url"], timeout=60).text if f["truncated"] else f["content"]
                break
            except Exception as e:
                log(f"Gist read failed ({e}), retrying in 10s")
                time.sleep(10)
        else:
            self.enabled = False
            log("Gist unreachable: NOT saving results to it this run, so the copy there stays intact")
            return
        for path, text in texts.items():
            with open(path, "w", newline="") as fh:
                fh.write(text)
            log(f"Restored {path} from the Gist ({len(text.splitlines()) - 1} signals)")

    def save(self, paths, file_lock):
        """Uploads the whole results files. A failed upload is simply retried with the next result."""
        if not self.enabled:
            return
        with self.lock:
            with file_lock:
                files = {}
                for path in paths:
                    if os.path.exists(path):
                        with open(path, newline="") as fh:
                            files[path] = {"content": fh.read()}
            try:
                requests.patch(f"{GIST_API}/{self.gist_id}", headers=self.headers, json={"files": files},
                               timeout=60).raise_for_status()
            except Exception as e:
                log(f"Gist save failed ({e}), will retry after the next result")

store = GistStore()

# ================== FEEDS ==================
class BinanceFeed:
    """One Binance connection with two BTCUSDT streams: calls on_candle(open_ts, open, close) per closed
    1-second candle, and on_trade(trade_ts, price) for every trade as it happens."""
    def __init__(self, on_candle, on_trade):
        self.on_candle, self.on_trade = on_candle, on_trade
        self.last_lag = None          # seconds between a candle closing and us receiving it
        self.last_trade_lag = None    # seconds between a trade and us receiving it
        self.connected_since = None   # when the current connection opened (None while disconnected)
        threading.Thread(target=self._run, daemon=True).start()

    def _run(self):
        while True:
            ws = websocket.WebSocketApp(BINANCE_WS, on_open=self._on_open, on_message=self._on_message,
                                        on_error=lambda w, e: log(f"Binance feed error: {e}"))
            ws.run_forever(ping_interval=20, ping_timeout=10)
            self.connected_since = None
            log("Binance feed disconnected, reconnecting in 2s")
            time.sleep(2)

    def _on_open(self, ws):
        self.connected_since = clock.now()

    def _on_message(self, ws, msg):
        m = json.loads(msg)
        d = m["data"]
        if m["stream"].endswith("@aggTrade"):
            t = d["T"] / 1000
            self.last_trade_lag = clock.now() - t
            self.on_trade(t, float(d["p"]))
        elif d["k"]["x"]:
            k = d["k"]
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
        self.rt_open = None   # the same from the trade stream (the window's first trade); False if missed
        self.trades_before_open = False
        self.signals = {}     # mode -> signal
        self.book = PolyBook(market["up_token"])

class PaperBot:
    def __init__(self):
        self.window = None
        self.csv_lock = threading.Lock()
        self.resolvers = []
        self.results = {mode: [] for mode in LOG_FILES}
        self.pending = {mode: {} for mode in LOG_FILES}   # mode -> slug -> signal still waiting for its market to resolve
        self.started = time.time()
        store.restore(LOG_FILES.values())
        for mode, path in LOG_FILES.items():
            if os.path.exists(path):
                with open(path, newline="") as f:
                    self.results[mode] = list(csv.DictReader(f))
                log(f"Loaded {len(self.results[mode])} earlier {mode} signals from {path}")
        self.feed = BinanceFeed(self.on_candle, self.on_trade)

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
        if w.open is False or "candle" in w.signals or t >= w.start + ENTRY_END:
            return
        move = c / w.open - 1
        if abs(move) >= MOVE_THRESHOLD:
            self.on_signal(w, "candle", c, t + 1, move)

    def on_trade(self, t, price):
        w = self.window
        if w is None:
            return
        if t < w.start:
            w.trades_before_open = True
            return
        if w.rt_open is None:
            # the window's first trade is its open, provided the stream was already running before the window began
            if w.trades_before_open and self.feed.connected_since < w.start:
                w.rt_open = price
            else:
                w.rt_open = False
                log(f"{w.slug}: missed the first BTC trade of the window, no realtime signal")
        if w.rt_open is False or "realtime" in w.signals or t >= w.start + ENTRY_END:
            return
        move = price / w.rt_open - 1
        if abs(move) >= MOVE_THRESHOLD:
            self.on_signal(w, "realtime", price, t, move)

    def on_signal(self, w, mode, price, ts, move):
        side = "Up" if move > 0 else "Down"
        candle = mode == "candle"
        sig = {"mode": mode, "slug": w.slug, "signal_utc": utc(ts),
               "sec_into_window": round(ts - w.start) if candle else round(ts - w.start, 2),
               "btc_open": w.open if candle else w.rt_open, "btc_price": price, "move_pct": round(move * 100, 4),
               "side": side, "btc_feed_lag": rnd(self.feed.last_lag if candle else self.feed.last_trade_lag, 2),
               "fills": {}}
        w.signals[mode] = sig

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
        log(f"{w.slug} {sig['mode']}: BTC {sig['move_pct']:+.3f}% at +{sig['sec_into_window']}s -> {sig['side']} | "
            f"bid/ask {f0['best_bid']}/{f0['best_ask']} (book age {age}) | avg fill {fills} | {sig['status'].upper()}")
        with self.csv_lock:
            self.pending[sig["mode"]][w.slug] = sig

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
            with self.csv_lock:
                for pending in self.pending.values():
                    pending.pop(w.slug, None)
        t = threading.Thread(target=run, daemon=True)
        t.start()
        self.resolvers.append(t)

    def record(self, w, winner):
        for mode, sig in w.signals.items():
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
                new = not os.path.exists(LOG_FILES[mode])
                with open(LOG_FILES[mode], "a", newline="") as fh:
                    wr = csv.DictWriter(fh, fieldnames=list(row))
                    if new:
                        wr.writeheader()
                    wr.writerow(row)
                self.results[mode].append(row)
                self.pending[mode].pop(w.slug, None)
            result = f"${row['pnl_0s']:+.2f}" if row["pnl_0s"] != "" else f"not traded ({sig['status']})"
            log(f"{w.slug} {mode}: {winner} won, {sig['side']} {'WIN' if won else 'LOSS'} -> {result}")
            log(f"TOTALS {mode}: " + self.summary(mode))
        store.save(LOG_FILES.values(), self.csv_lock)

    def summary(self, mode):
        results = self.results[mode]
        parts = []
        for d in DELAYS:
            pnls = [float(r[f"pnl_{d}s"]) for r in results if r.get(f"pnl_{d}s") not in ("", None)]
            if d == 0:
                wins = sum(p > 0 for p in pnls)
                parts.append(f"{len(results)} signals, {len(pnls)} paper trades, "
                             f"{(wins / len(pnls) * 100) if pnls else 0:.0f}% win, PnL {sum(pnls):+.2f}")
            else:
                parts.append(f"+{d}s late {sum(pnls):+.2f} ({len(pnls)})")
        return " | ".join(parts)

    def state(self):
        """Everything the dashboard shows, as JSON-friendly values."""
        modes = {}
        for mode in LOG_FILES:
            with self.csv_lock:
                rows = [dict(r) for r in self.results[mode]]
                pending = list(self.pending[mode].values())
            trades = []
            for r in rows:
                t = {}
                for k, v in r.items():
                    if k == "won":
                        t[k] = v in (True, "True")
                    elif v in ("", None):
                        t[k] = None
                    else:
                        try:
                            t[k] = float(v)
                        except (TypeError, ValueError):
                            t[k] = v
                trades.append(t)
            waiting = [{"slug": s["slug"], "signal_utc": s["signal_utc"], "sec_into_window": s["sec_into_window"],
                        "move_pct": s["move_pct"], "side": s["side"], "status": s.get("status"),
                        "best_bid": s["fills"][0]["best_bid"], "best_ask": s["fills"][0]["best_ask"],
                        **{f"fill_{d}s": s["fills"][d]["avg"] if d in s["fills"] else None for d in DELAYS}}
                       for s in pending]
            modes[mode] = {"trades": trades, "pending": waiting}
        w = self.window
        return {"now": clock.now(), "started": self.started, "window": w.slug if w else None,
                "binance_lag": self.feed.last_lag, "trade_lag": self.feed.last_trade_lag,
                "settings": {"move_pct": MOVE_THRESHOLD * 100, "entry_end": ENTRY_END, "max_fill": MAX_FILL,
                             "size_usd": SIZE_USD, "max_book_age": MAX_BOOK_AGE},
                "modes": modes}

# ================== WEB ==================
def start_web(bot, port):
    """Dashboard for the bot (Render web services set $PORT; locally use --port).
    /           dashboard (dashboard.html)
    /api/state  the data behind it, as JSON
    /csv        download paper_trades.csv (a free Render instance loses its files on restart)
    /csv/realtime  download paper_trades_realtime.csv"""
    if not port:
        return
    page = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard.html")

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            path = self.path.split("?")[0]
            csv_file = {"/csv": LOG_FILES["candle"], "/csv/realtime": LOG_FILES["realtime"]}.get(path)
            if path == "/api/state":
                body = json.dumps(bot.state()).encode()
                headers = {"Content-Type": "application/json", "Cache-Control": "no-store"}
            elif csv_file and os.path.exists(csv_file):
                with bot.csv_lock, open(csv_file, "rb") as f:
                    body = f.read()
                headers = {"Content-Type": "text/csv",
                           "Content-Disposition": f"attachment; filename={csv_file}"}
            elif path == "/" and os.path.exists(page):
                with open(page, "rb") as f:
                    body = f.read()
                headers = {"Content-Type": "text/html; charset=utf-8"}
            else:
                totals = "\n".join(f"{mode}: {bot.summary(mode)}" for mode in LOG_FILES)
                body = (f"Paper bot running. No real orders are ever sent.\n\n{totals}\n\n"
                        f"Results as CSV: /csv (candle), /csv/realtime\n").encode()
                headers = {"Content-Type": "text/plain; charset=utf-8"}
            self.send_response(200)
            for k, v in headers.items():
                self.send_header(k, v)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_HEAD(self):
            self.send_response(200)
            self.end_headers()

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("0.0.0.0", int(port)), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    log(f"Dashboard listening on port {port}")

    url = os.environ.get("RENDER_EXTERNAL_URL")
    if url:
        def keep_alive():
            while True:
                time.sleep(KEEP_ALIVE_MIN * 60)
                try:
                    requests.get(url, timeout=30)
                except Exception as e:
                    log(f"Keep-alive ping failed: {e}")
        threading.Thread(target=keep_alive, daemon=True).start()
        log(f"Keep-alive: pinging {url} every {KEEP_ALIVE_MIN} min so the instance doesn't sleep")

# ================== MAIN ==================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--minutes", type=float, default=0, help="stop after this many minutes (0 = run until Ctrl+C)")
    ap.add_argument("--port", type=int, default=None, help="serve the dashboard on this port (Render sets $PORT)")
    args = ap.parse_args()
    stop_at = time.time() + args.minutes * 60 if args.minutes else None

    log("Paper bot started. No real orders are ever sent. Ctrl+C to stop.")
    log(f"Signal: BTC {MOVE_THRESHOLD*100:.2f}% from open within {ENTRY_END}s | ${SIZE_USD}/trade | "
        f"max fill {MAX_FILL} | book must be <= {MAX_BOOK_AGE}s old | taker fee {bt.FEE_RATE} x p x (1-p) per share")
    log(f"Signal modes: {', '.join(LOG_FILES)} (results in {', '.join(LOG_FILES.values())})")
    clock.sync()
    bot = PaperBot()
    start_web(bot, args.port or os.environ.get("PORT"))
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
            if not w.signals:
                if w.open:
                    log(f"{slug}: no signal (BTC stayed within {MOVE_THRESHOLD*100:.2f}% for {ENTRY_END}s)")
            else:
                bot.resolve_later(w)
        log("Time limit reached, waiting for pending results...")
        for t in bot.resolvers:
            t.join(timeout=600)
    except KeyboardInterrupt:
        log("Stopped.")
    for mode in LOG_FILES:
        log(f"TOTALS {mode}: " + bot.summary(mode))

if __name__ == "__main__":
    main()
