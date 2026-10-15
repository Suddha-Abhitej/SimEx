# Architecture decision records

Each significant choice gets a short record: the context, the decision, and what it costs. New
records copy [`template.md`](template.md) and take the next number.

| # | Decision | Status |
| --- | --- | --- |
| [0001](0001-coinbase-as-trading-base.md) | Coinbase is the trading base | Accepted |
| [0002](0002-simulation-only-bring-your-own-key.md) | Simulation only; agents run on the user's side with their own key | Accepted |
| [0003](0003-live-forward-testing-is-the-only-ranked-score.md) | Live forward-testing is the only ranked score | Accepted |
| [0004](0004-decimal-for-all-money.md) | `Decimal` for all money | Accepted |
| [0005](0005-fills-at-arrival-with-anti-cheat-delay.md) | Fills at order arrival, with a stale-data guard and anti-cheat delay | Accepted |
| [0006](0006-zero-cost-hosting-oracle-plus-home-archive.md) | $0 hosting: Oracle Always Free for production, home server for archive | Accepted |
