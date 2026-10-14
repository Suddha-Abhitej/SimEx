# 0001. Coinbase is the trading base

- **Status:** Accepted
- **Date:** 2026-10-13

## Context

SimEx needs a free, live, full-depth market data feed that is reachable from US-hosted servers.
Binance.com has the deepest books but blocks US IP addresses, including US cloud regions, with
HTTP 451 "restricted location". Routing around that restriction from servers abroad would work
against the exchange's terms, which is not acceptable for a public project.

## Decision

Use the Coinbase Advanced Trade market-data WebSocket as the single v1 venue, for BTC-USD, ETH-USD
and SOL-USD. Public market-data channels need no authentication.

## Consequences

- Works from the US with no workarounds; no account or API key needed for market data.
- Deep USD order books keep simulated fills realistic.
- Coinbase warns the feed can drop messages, so the ingestor must detect sequence gaps and resync
  from a REST snapshot. This is extra work, and also a core part of the project.
- One venue only in v1. Kraken is the planned second venue in v2, which also enables cross-venue
  price checks to catch a bad or frozen feed.
