#!/usr/bin/env python3
"""
BSV perpetual position collector for bitcoinsv.it

Builds an exact liquidation map for the BSV perp on Hyperliquid from real
on-chain positions. Everything here is free and first-party to Hyperliquid:
no API key, no third-party aggregator, no paid archive.

Stage 0   address universe   leaderboard + vaults + addresses seen trading BSV
Stage 1   holder filter      HyperEVM position precompile via Multicall3
Stage 2   exact levels       clearinghouseState -> the exchange's own liquidationPx
Stage 3   publish            positions + bucketed map -> JSON for the page

Two things that are deliberate and must not be "optimised":

  * Stage 1 runs SEQUENTIALLY. Four parallel workers kill the whole run.
  * We never compute a liquidation price ourselves. Hyperliquid runs unified
    spot+perp margin, so spot USDC backs perp maintenance margin and the closed
    form is wrong for any account holding spot. The exchange reports its own
    liquidationPx per position; that is what we publish.
"""

import json
import sys
import time
from pathlib import Path

import requests

# ----------------------------------------------------------------------------

COIN = "BSV"

# Hyperliquid's metaAndAssetCtxs openInterest counts BOTH sides of the market:
# every isolated trade that moves it moves it by exactly twice the trade size
# (measured 2026-09-22 against the trades and asset-context feeds).
# Coverage is measured against one side.
HL_OI_SIDES = 2
ASSET_INDEX = 64  # BSV's index in meta.universe. Re-resolved at runtime.

INFO_URL = "https://api.hyperliquid.xyz/info"
EVM_RPC = "https://rpc.hyperliquid.xyz/evm"
LEADERBOARD_URL = "https://stats-data.hyperliquid.xyz/Mainnet/leaderboard"
VAULTS_URL = "https://stats-data.hyperliquid.xyz/Mainnet/vaults"

MULTICALL3 = "0xcA11bde05977b3631167028862bE2a173976CA11"
POSITION_PRECOMPILE = "0x0000000000000000000000000000000000000800"

# aggregate3((address,bool,bytes)[])
AGGREGATE3_SELECTOR = "82ad56cb"

CHUNK = 1000          # addresses per multicall
REQ_TIMEOUT = 90
PAUSE = 0.15          # between sequential multicalls

BASE = Path(__file__).resolve().parent
DATA = BASE / "data"
SEEN_ADDRESSES = DATA / "seen-addresses.txt"   # grown by the websocket collector
OUT_POSITIONS = DATA / "bsv-positions.json"
HISTORY_DIR = DATA / "history"

SESSION = requests.Session()
SESSION.headers.update({"Content-Type": "application/json"})


def log(msg):
    print("[%s] %s" % (time.strftime("%H:%M:%S"), msg), flush=True)


# --- helpers ---------------------------------------------------------------

def info(payload, retries=3):
    """POST to the Hyperliquid info endpoint with a small retry."""
    for attempt in range(retries):
        try:
            r = SESSION.post(INFO_URL, json=payload, timeout=REQ_TIMEOUT)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429:
                time.sleep(2 + attempt * 3)
                continue
            log("  info %s -> HTTP %s" % (payload.get("type"), r.status_code))
        except requests.RequestException as e:
            if attempt == retries - 1:
                log("  info %s failed: %s" % (payload.get("type"), e))
            time.sleep(1 + attempt * 2)
    return None


def to_int(word):
    """A 32-byte ABI word to a signed int.

    szi is an int64 sign-extended across the full 256 bits, so a raw unsigned
    read gives a number near 2**256 for any short position. Subtracting 2**256
    when the high bit is set is the whole fix, and getting it wrong yields
    silently absurd position sizes rather than an error.
    """
    v = int(word, 16) if isinstance(word, str) else word
    return v - (1 << 256) if v >= (1 << 255) else v


def encode_position_call(address, asset_index):
    """Calldata for the position precompile: (address, uint16)."""
    addr = address.lower().replace("0x", "").rjust(64, "0")
    idx = format(asset_index, "064x")
    return addr + idx


def build_aggregate3(addresses, asset_index):
    """Hand-roll aggregate3((address,bool,bytes)[]) calldata.

    Kept explicit rather than pulling in eth_abi so the cron has one dependency.
    """
    n = len(addresses)
    # Offsets are relative to the start of the array DATA area, which begins
    # after the length word, so the first element sits just past the offset
    # table. Including the length word here shifts every tuple by 32 bytes and
    # Multicall3 then reports success with empty returnData rather than failing.
    head_offset = n * 32

    offsets = []
    bodies = []
    for a in addresses:
        call = encode_position_call(a, asset_index)
        body = (
            POSITION_PRECOMPILE.lower().replace("0x", "").rjust(64, "0")
            + format(1, "064x")            # allowFailure = true
            + format(96, "064x")           # offset to bytes within this tuple
            + format(len(call) // 2, "064x")
            + call
        )
        offsets.append(head_offset + sum(len(b) // 2 for b in bodies))
        bodies.append(body)

    data = format(32, "064x") + format(n, "064x")
    data += "".join(format(o, "064x") for o in offsets)
    data += "".join(bodies)
    return "0x" + AGGREGATE3_SELECTOR + data


def decode_aggregate3(hexdata, count):
    """Decode Result[] = (bool success, bytes returnData)[].

    The trap: each Result tuple's bytes offset is relative to the START OF THAT
    TUPLE, not the start of the array. Reading it as array-relative produces
    plausible-looking garbage.
    """
    raw = hexdata[2:] if hexdata.startswith("0x") else hexdata
    words = [raw[i:i + 64] for i in range(0, len(raw), 64)]
    if len(words) < 2:
        return []

    arr_start = int(words[0], 16) // 32          # offset to the array
    n = int(words[arr_start], 16)
    n = min(n, count)
    results = []

    for i in range(n):
        tuple_off = arr_start + 1 + int(words[arr_start + 1 + i], 16) // 32
        success = int(words[tuple_off], 16) == 1
        if not success:
            results.append(None)
            continue
        bytes_off = tuple_off + int(words[tuple_off + 1], 16) // 32
        blen = int(words[bytes_off], 16)
        if blen == 0:
            results.append(None)
            continue
        payload = words[bytes_off + 1: bytes_off + 1 + (blen + 31) // 32]
        results.append(payload)

    return results


# --- stage 0: address universe ---------------------------------------------

def load_seen():
    if not SEEN_ADDRESSES.exists():
        return set()
    with open(SEEN_ADDRESSES, "r", encoding="utf-8") as f:
        return {ln.strip().lower() for ln in f if ln.strip()}


def fetch_universe():
    """leaderboard + vaults + everything the websocket collector has seen."""
    addrs = set()

    for name, url, key in (
        ("leaderboard", LEADERBOARD_URL, "ethAddress"),
        ("vaults", VAULTS_URL, None),
    ):
        try:
            t0 = time.time()
            r = requests.get(url, timeout=180)
            if r.status_code != 200:
                log("  %s -> HTTP %s, skipping" % (name, r.status_code))
                continue
            # Explicit utf-8: the default cp1252 on Windows throws on this file.
            payload = json.loads(r.content.decode("utf-8", errors="replace"))
            before = len(addrs)
            addrs |= harvest_addresses(payload, key)
            log("  %s: %.1f MB in %.1fs, +%d addresses"
                % (name, len(r.content) / 1e6, time.time() - t0, len(addrs) - before))
        except Exception as e:
            log("  %s failed: %s" % (name, e))

    seen = load_seen()
    before = len(addrs)
    addrs |= seen
    log("  websocket-seen file: %d addresses, +%d new" % (len(seen), len(addrs) - before))

    return sorted(addrs)


def harvest_addresses(obj, key, out=None):
    """Walk the JSON and pull anything that looks like an address."""
    if out is None:
        out = set()
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(v, str) and v.startswith("0x") and len(v) == 42:
                if key is None or k == key or "address" in k.lower() or "leader" in k.lower():
                    out.add(v.lower())
            else:
                harvest_addresses(v, key, out)
    elif isinstance(obj, list):
        for v in obj:
            harvest_addresses(v, key, out)
    return out


# --- stage 1: who holds BSV ------------------------------------------------

def resolve_asset_index():
    """BSV's index in the perp universe, or None.

    Never falls back to a hardcoded index: a wrong index sweeps the wrong
    market and would publish a confident, wrong file.
    """
    for payload in ({"type": "meta"}, {"type": "metaAndAssetCtxs"}):
        d = info(payload)
        uni = (d[0] if isinstance(d, list) else d or {}).get("universe", []) if d else []
        for i, m in enumerate(uni):
            if m.get("name") == COIN:
                log("  %s resolved to universe index %d (maxLeverage %s)"
                    % (COIN, i, m.get("maxLeverage")))
                return i
    return None


def find_holders(addresses, asset_index):
    """Sequential Multicall3 sweep. Parallelising this kills the run."""
    holders = []
    chunks = [addresses[i:i + CHUNK] for i in range(0, len(addresses), CHUNK)]
    failures = 0

    for i, chunk in enumerate(chunks, 1):
        payload = {
            "jsonrpc": "2.0", "id": i, "method": "eth_call",
            "params": [{"to": MULTICALL3, "data": build_aggregate3(chunk, asset_index)}, "latest"],
        }
        try:
            r = requests.post(EVM_RPC, json=payload, timeout=REQ_TIMEOUT)
            res = r.json().get("result")
            if not res:
                failures += 1
                continue
            for addr, words in zip(chunk, decode_aggregate3(res, len(chunk))):
                if not words:
                    continue
                szi = to_int(words[0])
                if szi != 0:
                    holders.append(addr)
        except Exception as e:
            failures += 1
            if failures <= 3:
                log("  chunk %d/%d failed: %s" % (i, len(chunks), e))

        if i % 10 == 0 or i == len(chunks):
            log("  swept %d/%d chunks, %d holders so far" % (i, len(chunks), len(holders)))
        time.sleep(PAUSE)

    if failures:
        log("  %d/%d chunks failed" % (failures, len(chunks)))
    return holders, failures, len(chunks)


# --- stage 2: exact liquidation prices -------------------------------------

def fetch_positions(holders):
    positions = []
    for i, addr in enumerate(holders, 1):
        st = info({"type": "clearinghouseState", "user": addr})
        if not st:
            continue
        for ap in st.get("assetPositions", []):
            p = ap.get("position", {})
            if p.get("coin") != COIN:
                continue
            szi = float(p.get("szi", 0))
            if szi == 0:
                continue
            liq = p.get("liquidationPx")
            positions.append({
                "address": addr,
                "szi": szi,
                "side": "long" if szi > 0 else "short",
                "entryPx": float(p.get("entryPx") or 0),
                "positionValue": float(p.get("positionValue") or 0),
                "unrealizedPnl": float(p.get("unrealizedPnl") or 0),
                "liquidationPx": float(liq) if liq is not None else None,
                "leverage": (p.get("leverage") or {}).get("value"),
                "leverageType": (p.get("leverage") or {}).get("type"),
                "maxLeverage": p.get("maxLeverage"),
                "cumFundingSinceOpen": float((p.get("cumFunding") or {}).get("sinceOpen") or 0),
            })
        if i % 20 == 0:
            log("  read %d/%d holder states" % (i, len(holders)))
        time.sleep(0.08)
    return positions


# --- stage 3: publish ------------------------------------------------------

def build_map(positions, mark_px, n_buckets=60, span=0.6):
    """Bucket liquidation prices into price bands weighted by position value.

    This is the archived summary. The terminal rebuilds its own map from the
    raw positions against the live mark, so the two can differ slightly when
    the price has moved since the sweep.

    Every total is split by side of the mark AND by whether it falls inside
    the charted window, because a short parked at $1,100 is real but is not
    risk anyone near $21 needs to plan around, and lumping it into "above"
    made that total read as imminent when it was not.
    """
    lo, hi = mark_px * (1 - span), mark_px * (1 + span)
    width = (hi - lo) / n_buckets
    buckets = [{"lo": lo + i * width, "hi": lo + (i + 1) * width,
                "long": 0.0, "short": 0.0, "n": 0, "maxPos": None}
               for i in range(n_buckets)]

    in_below = in_above = off_below = off_above = 0.0
    for p in positions:
        liq, val = p.get("liquidationPx"), p.get("positionValue") or 0
        if not liq or liq <= 0 or val <= 0:
            continue
        if liq < lo:
            off_below += val
            continue
        if liq >= hi:
            off_above += val
            continue
        if liq < mark_px:
            in_below += val
        else:
            in_above += val
        b = buckets[min(int((liq - lo) / width), n_buckets - 1)]
        b[p["side"]] += val
        b["n"] += 1
        if b["maxPos"] is None or val > b["maxPos"]["positionValue"]:
            b["maxPos"] = {"positionValue": val, "liquidationPx": liq, "side": p["side"]}

    magnet = None
    for b in buckets:
        tot = b["long"] + b["short"]
        if tot > 0 and (magnet is None or tot > magnet["value"]):
            magnet = {"price": round((b["lo"] + b["hi"]) / 2, 4), "value": round(tot, 2),
                      "n": b["n"], "lo": round(b["lo"], 4), "hi": round(b["hi"], 4)}
            # A single position is a level, not a cluster: keep its exact
            # liquidation price rather than the bucket midpoint.
            if b["n"] == 1 and b["maxPos"]:
                magnet["exactPx"] = round(b["maxPos"]["liquidationPx"], 4)
                magnet["side"] = b["maxPos"]["side"]

    for b in buckets:
        b.pop("maxPos", None)

    return {
        "buckets": [b for b in buckets if b["n"]],
        "bucketWidth": round(width, 4),
        "windowLo": round(lo, 4),
        "windowHi": round(hi, 4),
        "inViewBelow": round(in_below, 2),
        "inViewAbove": round(in_above, 2),
        "offBelow": round(off_below, 2),
        "offAbove": round(off_above, 2),
        # kept for older copies of the terminal
        "totalBelow": round(in_below + off_below, 2),
        "totalAbove": round(in_above + off_above, 2),
        "offChart": round(off_below + off_above, 2),
        "magnetZone": magnet,
    }


def fetch_caps(prev=None):
    """Leverage tiers for the centralised venues, fetched server-side.

    Binance publishes its BSVUSDT brackets only through the endpoint its own
    website uses, which sends no CORS header, so a browser cannot read it and
    the terminal gets it from this file instead. Bybit is public and CORS-open
    but changes rarely, so it rides along here too. Hyperliquid is read live
    by the terminal from meta. A venue that fails is simply omitted and the
    terminal shows a dash for it.

    Each venue carries the time it was last read. When a fetch fails (both
    venues refuse some regions), the previous reading is kept with its old
    time, and the terminal stops trusting it after a week.
    """
    caps = {}
    now = int(time.time() * 1000)
    prev = prev or {}
    try:
        r = requests.get(
            "https://www.binance.com/bapi/futures/v1/friendly/future/common/brackets",
            params={"symbol": "BSVUSDT"},
            headers={"User-Agent": "Mozilla/5.0 (bitcoinsv.it data collector)"},
            timeout=30)
        rb = r.json()["data"]["brackets"][0]["riskBrackets"]
        tiers = [{"upTo": t["bracketNotionalCap"], "maxLev": t["maxOpenPosLeverage"]} for t in rb]
        caps["BinPerp"] = {"maxLev": tiers[0]["maxLev"], "upTo": tiers[0]["upTo"],
                           "tiers": tiers, "source": "binance.com brackets", "checked": now}
    except Exception as e:
        log("  Binance leverage tiers unavailable: %s" % e)
        if prev.get("BinPerp"):
            caps["BinPerp"] = prev["BinPerp"]
    try:
        r = requests.get("https://api.bybit.com/v5/market/risk-limit",
                         params={"category": "linear", "symbol": "BSVUSDT"}, timeout=30)
        rl = r.json()["result"]["list"]
        tiers = [{"upTo": float(t["riskLimitValue"]), "maxLev": float(t["maxLeverage"])} for t in rl]
        caps["BybitPerp"] = {"maxLev": tiers[0]["maxLev"], "upTo": tiers[0]["upTo"],
                             "tiers": tiers, "source": "bybit risk-limit", "checked": now}
    except Exception as e:
        log("  Bybit leverage tiers unavailable: %s" % e)
        if prev.get("BybitPerp"):
            caps["BybitPerp"] = prev["BybitPerp"]
    return caps


def main():
    DATA.mkdir(parents=True, exist_ok=True)
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    t_start = time.time()

    # The last published file: the publish guard compares against it, and
    # leverage tiers carry over from it when a venue refuses the request.
    prev = {}
    try:
        with open(OUT_POSITIONS, "r", encoding="utf-8") as f:
            prev = json.load(f)
    except Exception:
        pass

    log("Stage 0: building address universe")
    universe = fetch_universe()
    log("  universe: %d addresses" % len(universe))
    if not universe:
        log("ABORT: empty universe")
        return 1

    asset_index = resolve_asset_index()
    if asset_index is None:
        log("ABORT: could not find %s in the Hyperliquid universe; previous file kept" % COIN)
        return 1

    log("Stage 1: sweeping for %s holders (sequential, ~%d chunks)"
        % (COIN, (len(universe) + CHUNK - 1) // CHUNK))
    t0 = time.time()
    holders, failed_chunks, total_chunks = find_holders(universe, asset_index)
    log("  %d holders found in %.1fs" % (len(holders), time.time() - t0))

    log("Stage 2: reading exact liquidation levels")
    positions = fetch_positions(holders)
    log("  %d open %s positions" % (len(positions), COIN))

    ctx = info({"type": "metaAndAssetCtxs"})
    mark_px = open_interest = funding = day_vlm = 0.0
    if ctx:
        uni = ctx[0].get("universe", [])
        if asset_index >= len(uni) or uni[asset_index].get("name") != COIN:
            log("ABORT: universe index %d is no longer %s; previous file kept" % (asset_index, COIN))
            return 1
        c = ctx[1][asset_index]
        mark_px = float(c.get("markPx") or 0)
        open_interest = float(c.get("openInterest") or 0)
        funding = float(c.get("funding") or 0)
        day_vlm = float(c.get("dayNtlVlm") or 0)

    per_side = open_interest / HL_OI_SIDES
    seen_long = sum(p["szi"] for p in positions if p["szi"] > 0)
    # Positions whose liquidationPx is null are real and seen, but cannot be
    # charted: their account collateral is large enough that no BSV price
    # would force them closed. They count as seen and are reported
    # separately, never silently folded into the mapped share.
    mapped = [p for p in positions if p.get("liquidationPx")]
    no_liq = [p for p in positions if not p.get("liquidationPx")]
    mapped_long = sum(p["szi"] for p in mapped if p["szi"] > 0)
    mapped_short = -sum(p["szi"] for p in mapped if p["szi"] < 0)
    seen_short = -sum(p["szi"] for p in positions if p["szi"] < 0)

    out = {
        "coin": COIN,
        "updated": int(time.time() * 1000),
        "markPx": mark_px,
        "openInterestCoins": open_interest,          # as reported: both sides summed
        "openInterestSides": HL_OI_SIDES,
        "openInterestCoinsPerSide": round(per_side, 2),
        "openInterestUsd": round(per_side * mark_px, 2),   # one side, comparable to Binance
        "fundingHourly": funding,
        "dayNtlVlm": day_vlm,
        "coverage": {
            "addressesScanned": len(universe),
            "holdersFound": len(holders),
            "positions": len(positions),
            "longCoinsSeen": round(seen_long, 2),
            "shortCoinsSeen": round(seen_short, 2),
            "longPctOfOi": round(seen_long / per_side * 100, 1) if per_side else None,
            "shortPctOfOi": round(seen_short / per_side * 100, 1) if per_side else None,
            "longPctMapped": round(mapped_long / per_side * 100, 1) if per_side else None,
            "shortPctMapped": round(mapped_short / per_side * 100, 1) if per_side else None,
            "noLiqPositions": len(no_liq),
            "noLiqValue": round(sum(p["positionValue"] or 0 for p in no_liq), 2),
            "noLiqLongCoins": round(sum(p["szi"] for p in no_liq if p["szi"] > 0), 2),
            "noLiqShortCoins": round(-sum(p["szi"] for p in no_liq if p["szi"] < 0), 2),
            "chunksFailed": failed_chunks,
            "chunksTotal": total_chunks,
        },
        # Addresses stay out of the published file: the map needs only sizes
        # and liquidation prices.
        "positions": [{k: v for k, v in p.items() if k != "address"}
                      for p in sorted(positions, key=lambda p: -(p["positionValue"] or 0))],
        "map": build_map(positions, mark_px) if mark_px else None,
        "caps": fetch_caps(prev.get("caps")),
        "runSeconds": round(time.time() - t_start, 1),
    }

    # Publish guard: never replace a good file with a broken run.
    had_positions = bool(prev.get("positions"))
    if not mark_px or failed_chunks == total_chunks or (not positions and had_positions):
        log("ABORT: run looks broken (mark %s, %d/%d chunks failed, %d positions); previous file kept"
            % (mark_px, failed_chunks, total_chunks, len(positions)))
        return 1

    with open(OUT_POSITIONS, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=1)

    stamp = time.strftime("%Y-%m-%dT%H", time.gmtime())
    with open(HISTORY_DIR / ("bsv-%s.json" % stamp), "w", encoding="utf-8") as f:
        json.dump({k: v for k, v in out.items() if k != "positions"}, f, indent=1)

    cov = out["coverage"]
    log("Done in %.1fs. mark $%.4f, OI %s BSV per side ($%s)"
        % (out["runSeconds"], mark_px, f"{per_side:,.0f}", f"{per_side*mark_px:,.0f}"))
    log("  coverage: %s%% of long OI, %s%% of short OI seen; %s%% / %s%% have a liquidation price"
        % (cov["longPctOfOi"], cov["shortPctOfOi"], cov["longPctMapped"], cov["shortPctMapped"]))
    log("  %d positions have no liquidation price ($%s)"
        % (cov["noLiqPositions"], f"{cov['noLiqValue']:,.0f}"))
    if out["map"] and out["map"]["magnetZone"]:
        m = out["map"]["magnetZone"]
        log("  magnet zone $%.2f holding $%s" % (m["price"], f"{m['value']:,.0f}"))
    log("  wrote %s" % OUT_POSITIONS)
    return 0


if __name__ == "__main__":
    sys.exit(main())
