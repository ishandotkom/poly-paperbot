# Polymarket BTC 5-minute: backtests and paper bot

Research code for Polymarket's "Bitcoin Up or Down – 5 minute" markets.
The paper bot only reads public data. **It never places a real order.**

## Files

| File | What it does |
|---|---|
| `paper_bot.py` | Live paper trading. When BTC moves 0.03% from a window's open on Binance within the first 2 minutes, it prices a $20 market buy of that side from the live Polymarket order book (at the signal and 1–3 s later), then records the result when the market resolves. |
| `dashboard.html` | The dashboard `paper_bot.py` serves: summary stats, a cumulative PnL chart, and every signal. |
| `backtest_approx.py` | Backtest on 1-minute data. Also holds the shared helpers `paper_bot.py` imports (market lookup, fee formula, trade size), so keep it in the same folder. |
| `lag_test.py` | Backtest of the same lag rule on per-second data (Polymarket trades + Binance 1-second candles). |

## Setup

Python 3.10 or newer.

```bash
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

## Run the paper bot

```bash
python paper_bot.py               # runs until Ctrl+C
python paper_bot.py --minutes 60  # stops after an hour, then waits for pending results
python paper_bot.py --port 8000   # also serves the dashboard at http://localhost:8000
```

Every signal is appended to `paper_trades.csv` once its market resolves: the BTC move, the side, the order-book
fill at 0/1/2/3 seconds after the signal (`fill_0s` … `fill_3s`), the winner, and the PnL for each delay
(`pnl_0s` … `pnl_3s`, after Polymarket's taker fee). Running totals are printed after each result.

**It needs a fast, stable connection.** Each signal line shows `book age`: how old the order book was, in
exchange time, at the signal. If it is over 2 seconds the signal is logged as `STALE BOOK` and not counted.
On a slow connection the Polymarket feed falls behind, so every signal ends up stale. A small cloud server
works better than a home connection.

Settings (threshold, max fill price, trade size, allowed book age) are at the top of `paper_bot.py`;
trade size is `SIZE_USD` in `backtest_approx.py`.

## Run it on Render (free plan)

Create a **Web Service** from this repo:

- Build command: `pip install -r requirements.txt`
- Start command: `python paper_bot.py`
- Region: Singapore worked well (Binance feed lag ~0.05 s, order book age ~0.1 s).

Render sets `PORT`, so the bot serves the dashboard at the service URL (stats, PnL chart, every signal;
refreshes every 30 s) and the results file at `/csv`. A free instance loses its files on every restart or deploy, so download `/csv` from
time to time, and always before pushing a change.

A free instance sleeps after 15 minutes without traffic, which would stop the bot. To prevent that, the bot
requests its own URL (`RENDER_EXTERNAL_URL`) every 10 minutes. If it still goes to sleep, set up a free
uptime monitor to open the URL every 5–10 minutes.

`Clock sync failed` in the log can be ignored on a server: the server's own clock is accurate.

## Run the backtests

```bash
python backtest_approx.py   # ~6 minutes the first time, seconds after that (cached)
python lag_test.py          # ~1 hour the first time (downloads per-second data), then cached
```

## Data notes

- **Fees:** Polymarket crypto markets charge takers `shares × 0.07 × p × (1 − p)`: 3.5% of the trade value at
  50¢, about 2.1% at 70¢ ([docs](https://docs.polymarket.com/trading/fees)). Makers pay nothing.
- **Trade timestamps:** the timestamps from `data-api.polymarket.com/trades` are Polygon block times, about
  2 seconds after the trade actually matched. `lag_test.py` corrects for this (`SETTLE_DELAY`).
- **Price history:** `prices-history` buckets are at least 60 seconds, and each point is the price at its
  timestamp. The backtest only uses points at or before the decision time.

Not financial advice. Check Polymarket's terms for your country before trading real money.
