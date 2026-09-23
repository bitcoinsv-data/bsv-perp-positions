# Bitcoin SV (BSV) perpetual positions on Hyperliquid

An hourly snapshot of the open positions on the Bitcoin SV (BSV) perpetual on
Hyperliquid, with the liquidation price the exchange itself reports for each
one. It feeds the liquidation panel of the live
[Bitcoin SV funding rate and open interest terminal](https://bitcoinsv.it/bsv-funding-rate/)
on bitcoinsv.it.

**Latest file:** https://bitcoinsv-data.github.io/bsv-perp-positions/bsv-positions.json
(refreshed every hour)

## How it is built

Every source is free, public and first-party to Hyperliquid:

1. **Address universe.** Hyperliquid's public leaderboard and vault list, plus
   every address seen on either side of a BSV trade.
2. **Holders.** The HyperEVM position precompile, read through Multicall3.
3. **Liquidation prices.** `clearinghouseState`, which returns the exchange's
   own `liquidationPx` for each position. No liquidation price is computed
   here: Hyperliquid's unified spot and perp margin makes the textbook formula
   wrong for any account holding spot.

Leverage tiers for Binance and Bybit come from their own public endpoints.

## Fields

| Field | Meaning |
|---|---|
| `updated` | Time of the snapshot, milliseconds since 1970 (UTC) |
| `markPx` | Hyperliquid mark price at the snapshot |
| `openInterestCoinsPerSide`, `openInterestUsd` | Open interest for one side of the market. Hyperliquid reports both sides summed; this file halves it so it compares with Binance |
| `coverage` | Share of open interest seen (`longPctOfOi`, `shortPctOfOi`) and share with a liquidation price (`longPctMapped`, `shortPctMapped`) |
| `positions` | Side, size (`szi`), entry price, leverage, `liquidationPx` and value of each position. Addresses are not included |
| `map` | Positions grouped into price bands by liquidation price |
| `caps` | Binance and Bybit leverage tiers, each with the time it was last read (`checked`) |

## Limits

Coverage is not complete. An account that never ranked on the leaderboard and
has not traded BSV since collection started is not seen. The `coverage` fields
state how much is. Some positions carry no liquidation price because their
account holds enough collateral that no BSV price would force them closed;
they are counted in `coverage.noLiqPositions` and left off the map.

## Licence

MIT
