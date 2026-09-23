#!/usr/bin/env python3
"""
BSV address collector for bitcoinsv.it

Subscribes to the Hyperliquid trades feed for BSV and records every address
that appears on either side of a fill. This is the piece that closes the
coverage gap: the public leaderboard only lists ranked accounts, so a sweep
built on it alone sees roughly a third of open interest. Every BSV trade
reveals two addresses, so coverage compounds the longer this runs.

Run it as a long-lived service. It appends to a dedupe file that
collect_positions.py unions into its address universe on every run.

    python watch_addresses.py              # run until interrupted
    python watch_addresses.py --minutes 30 # run for a fixed window

Restart-safe: the seen file is read on start and only new addresses are
appended, so stopping and restarting never loses or duplicates work.
"""

import argparse
import asyncio
import json
import signal
import sys
import time
from pathlib import Path

import websockets

WS_URL = "wss://api.hyperliquid.xyz/ws"
COIN = "BSV"

BASE = Path(__file__).resolve().parent
DATA = BASE / "data"
SEEN_FILE = DATA / "seen-addresses.txt"
TRADES_LOG = DATA / "bsv-trades.jsonl"
LIQ_LOG = DATA / "bsv-liquidations.jsonl"

stop = False


def log(msg):
    print("[%s] %s" % (time.strftime("%H:%M:%S"), msg), flush=True)


def load_seen():
    if not SEEN_FILE.exists():
        return set()
    with open(SEEN_FILE, "r", encoding="utf-8") as f:
        return {ln.strip().lower() for ln in f if ln.strip()}


async def collect(deadline):
    global stop
    DATA.mkdir(parents=True, exist_ok=True)

    seen = load_seen()
    started_with = len(seen)
    log("starting with %d known addresses" % started_with)

    new_count = trade_count = 0
    backoff = 1

    while not stop and (deadline is None or time.time() < deadline):
        try:
            async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20) as ws:
                await ws.send(json.dumps({
                    "method": "subscribe",
                    "subscription": {"type": "trades", "coin": COIN},
                }))
                backoff = 1
                log("subscribed to %s trades" % COIN)

                while not stop and (deadline is None or time.time() < deadline):
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=30)
                    except asyncio.TimeoutError:
                        continue

                    try:
                        msg = json.loads(raw)
                    except ValueError:
                        continue
                    if msg.get("channel") != "trades":
                        continue

                    fresh = []
                    for t in msg.get("data", []):
                        trade_count += 1

                        with open(TRADES_LOG, "a", encoding="utf-8") as f:
                            f.write(json.dumps({
                                "t": t.get("time"), "px": t.get("px"),
                                "sz": t.get("sz"), "side": t.get("side"),
                                "tid": t.get("tid"),
                            }) + "\n")

                        # Hyperliquid tags a forced close in the trade payload.
                        # Recording it here is the only free way to keep a real
                        # BSV liquidation history: nobody sells one and the
                        # exchange does not serve it retrospectively.
                        if t.get("liquidation") or t.get("liquidationMarkPx"):
                            with open(LIQ_LOG, "a", encoding="utf-8") as f:
                                f.write(json.dumps(t) + "\n")
                            log("  liquidation: %s %s @ %s"
                                % (t.get("side"), t.get("sz"), t.get("px")))

                        for a in (t.get("users") or []):
                            a = (a or "").lower()
                            if a.startswith("0x") and len(a) == 42 and a not in seen:
                                seen.add(a)
                                fresh.append(a)

                    if fresh:
                        with open(SEEN_FILE, "a", encoding="utf-8") as f:
                            for a in fresh:
                                f.write(a + "\n")
                        new_count += len(fresh)
                        log("  +%d new (%d trades, %d new this session, %d total)"
                            % (len(fresh), trade_count, new_count, len(seen)))

        except Exception as e:
            if stop:
                break
            log("connection lost (%s), retrying in %ds" % (type(e).__name__, backoff))
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

    log("stopped. %d trades seen, %d new addresses, %d known total (was %d)"
        % (trade_count, new_count, len(seen), started_with))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--minutes", type=float, default=None,
                    help="run for this long then exit; omit to run until interrupted")
    args = ap.parse_args()

    def handle(*_):
        global stop
        stop = True
        log("shutting down")

    signal.signal(signal.SIGINT, handle)
    try:
        signal.signal(signal.SIGTERM, handle)
    except (AttributeError, ValueError):
        pass

    deadline = time.time() + args.minutes * 60 if args.minutes else None
    asyncio.run(collect(deadline))
    return 0


if __name__ == "__main__":
    sys.exit(main())
