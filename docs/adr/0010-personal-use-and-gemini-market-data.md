# 0010. Personal research tool on Gemini market data

- **Status:** Accepted
- **Date:** 2026-10-14
- **Supersedes:** 0001 (Coinbase is the trading base). Narrows 0002 and 0003 (see Decision).

## Context

SimEx was planned as a public arena: anyone plugs in an AI trader, trades against the live
Coinbase book, and appears on a public leaderboard, with recordings available to download. Reading
the exchanges' market-data terms (2026-10-14) ruled that out at $0:

- **Coinbase** Market Data Terms (updated 2026-08-07) allow personal or research use only, forbid
  redistribution and display including derived works, and explicitly forbid using the data to
  validate or benchmark any AI agent. That last clause applies even to private use, so Coinbase
  data cannot drive SimEx's agents at all.
- **Kraken** allows use for "your own benefit" only, with no third-party apps or competing products
  without consent.
- **Gemini** allows "personal and/or internal use"; redistribution needs a separate agreement
  (bizdev@gemini.com). It has no AI or benchmarking clause.
- **Paid licences** that allow public redistribution are quoted by sales. The one published figure
  found was about $1,750 a month (Databento Plus, US equities and futures, no crypto). That breaks
  the $0 budget.

The engineering (streaming ingest, gap resync, order book, matching engine, ledger, evaluation) is
the same whether one person or many use it. Only the audience changes.

## Decision

- **Audience: one user, the owner.** SimEx is a personal research tool for testing the owner's own
  AI agents against the live market. There is no public leaderboard, no public sign-up, no
  downloadable recordings, and no public page that shows market data or results derived from it.
- **Any web UI is private.** The dashboard and API are reachable only by the owner, behind
  Cloudflare Access (or an SSH tunnel), never as an open public page.
- **Code stays public** under Apache-2.0 as a portfolio piece. The repository never contains
  captured market data: no recordings, no feed dumps, and no fixtures copied from a live feed.
  Test fixtures are hand-written or synthetic.
- **Live venue: Gemini** public market-data WebSocket (`wss://ws.gemini.com`, no authentication),
  pairs BTCUSD, ETHUSD and SOLUSD.
- **Market sources are pluggable.** The ingestor gets a venue adapter interface. Every adapter
  publishes the same envelope and topics (ADR 0007), with the venue recorded, so the order book,
  matching engine and everything downstream never depend on one exchange.
- **The Coinbase adapter stays in the repo but is off by default.** It may run for feed-quality
  research (lag and gap measurements), which its terms allow, but its data must never feed agents,
  the matching engine or scoring.
- **Public showcase uses no exchange data.** Write-ups and demos show architecture and system
  metrics (throughput, lag, gaps, latency, reconciliation). Anything showing prices or agent
  results either uses a synthetic market source (a possible later adapter) or is checked against
  Gemini's terms first.
- **ADR 0002 narrowed:** simulation only and never running others' code still hold; "anyone" is now
  the owner. **ADR 0003 narrowed:** live forward-testing is still the only score that counts, on a
  private scoreboard instead of a public leaderboard.

## Consequences

- $0 stays $0, and the project stays within every exchange's terms as read on 2026-10-14. Terms
  change (Coinbase's did in August 2026), so they are re-read before any new use of the data.
- The ingestor needs a Gemini adapter. Gemini's feed differs from Coinbase's: depth diffs carry
  first and last update IDs (`U`, `u`) instead of a per-connection `sequence_num`, and the initial
  book comes from a connection parameter (`?snapshot=-1`). Gap detection and resync (ADR 0008) must
  be re-mapped and confirmed against the live feed.
- Work already done on the Coinbase ingestor (envelope, gap resync, snapshot refresh) carries over
  as the design the Gemini adapter follows.
- Gemini's books are thinner than Coinbase's, so simulated fills for large orders get worse prices.
  That is realistic for the venue being simulated.
- The portfolio loses "real users" as a metric. It keeps the engineering story, and adds a
  pluggable-source design and a documented licensing review.
- Opening SimEx to others later needs one of: a Gemini redistribution agreement, a paid licence, or
  a synthetic market. The pluggable-source design keeps all three possible without a rewrite.
