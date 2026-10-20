# Architecture decision records

Each significant choice gets a short record: the context, the decision, and what it costs. New
records copy [`template.md`](template.md) and take the next number.

| # | Decision | Status |
| --- | --- | --- |
| [0001](0001-coinbase-as-trading-base.md) | Coinbase is the trading base | Superseded by 0010 |
| [0002](0002-simulation-only-bring-your-own-key.md) | Simulation only; agents run on the user's side with their own key | Accepted, narrowed by 0010 |
| [0003](0003-live-forward-testing-is-the-only-ranked-score.md) | Live forward-testing is the only ranked score | Accepted, narrowed by 0010 |
| [0004](0004-decimal-for-all-money.md) | `Decimal` for all money | Accepted |
| [0005](0005-fills-at-arrival-with-anti-cheat-delay.md) | Fills at order arrival, with a stale-data guard and anti-cheat delay | Superseded by 0016 |
| [0006](0006-zero-cost-hosting-oracle-plus-home-archive.md) | $0 hosting: Oracle Always Free for production, home server for archive | Accepted |
| [0007](0007-market-data-envelope-and-topics.md) | Market-data envelope and Kafka topics | Accepted |
| [0008](0008-gap-resync-markers-and-gap-records.md) | Gap resync: in-band markers on md.level2, gap records on md.gaps | Accepted |
| [0009](0009-book-cold-start-snapshot-refresh.md) | Book cold start: staggered snapshot refresh and a time-based seek | Accepted |
| [0010](0010-personal-use-and-gemini-market-data.md) | Personal research tool on Gemini market data | Accepted |
| [0011](0011-venue-adapters-and-gemini-gap-rule.md) | Venue adapters, envelope v3, and Gemini's gap rule | Accepted |
| [0012](0012-order-book-service-valkey-and-audit.md) | Order book library and service, Valkey book records, and the book audit | Accepted |
| [0013](0013-recorder-parquet-layout-exactly-once-retention.md) | Recorder: hourly zstd Parquet, exactly once, local retention | Accepted |
| [0014](0014-order-model-and-fill-model.md) | Order model, per-account balances, and the fill model | Accepted |
| [0015](0015-live-runner-merge-and-watermark.md) | Live exchange runner: merge by received time, release to a watermark | Accepted |
| [0016](0016-stale-data-guard-and-fill-timing.md) | Complete stale-data guard and fill timing rule set | Accepted |
