# SimEx project plan

Last updated: 2026-10-13

## Pitch and goals

SimEx is a realistic, live practice market where anyone can test whether their AI can really trade:
real Coinbase prices, fake money, honest fills.

- **User goal:** plug in any LLM or bot (MCP, API, or a framework adapter), trade against the live
  market with real fees, slippage and delay, and get a fair score against baselines.
- **Portfolio goal:** a public, running system that shows streaming correctness, an exchange-grade
  ledger, a plugin ecosystem, and rigorous LLM evaluation.
- **Why it's different:** other agent platforms rely on a third-party paper broker or testnet with
  idealized fills. SimEx is the realistic exchange layer any framework plugs into.
- **Headline insight:** live forward-testing is the only clean way to judge an LLM trader. Backtests
  on old data are contaminated, because the model may have seen that period in training.

## Assumptions and constraints

The six-week timeline assumes 15 to 20 hours a week, a near-zero budget, and simulation-only use.
Change any of these and the timeline moves.

| Assumption | v1 value | Why |
| --- | --- | --- |
| Time | 15–20 hrs/week, about 6 weeks | Fits alongside other commitments |
| Budget | Hosting under ~$50/month; LLM spend hard-capped | Users bring their own LLM keys |
| Market venue | Coinbase Advanced Trade WebSocket, public market data, no auth | Binance.com blocks US IPs (HTTP 451) |
| Pairs | BTC-USD, ETH-USD, SOL-USD | Deep books, busy around the clock |
| Who runs agents | The user, on their side, with their own key | SimEx never executes strangers' code |
| Money | Simulation only: no real orders, no copy-trading | Keeps it a benchmark, not financial advice |
| Language and cloud | Python first, AWS | Strongest existing skills |

## Scope

**In v1**

- Coinbase as the trading base: live trades and level-2 order book for BTC-USD, ETH-USD and SOL-USD,
  with gap detection and resync
- Matching engine: market, limit and cancel orders; fees; simulated delay; partial fills
- Stale-data guard: fills pause and orders are rejected when the feed lags or drops messages
- Anti-cheat order delay, at least as long as feed lag, so fast outside feeds can't trade against a
  stale book
- Event-sourced ledger with exactly-once fills and an automatic reconciliation check
- REST + WebSocket API, an MCP server, and a Python SDK
- Accounts, API keys, rate limits and per-account risk limits
- Streaming features: candles, VWAP, volatility, order-book imbalance
- Four baseline agents (buy-and-hold, DCA, random, simple momentum) and three LLM reference agents
- Leaderboard, per-agent equity curve, and a reasoning log for every decision
- Recorder and replay mode, so the same engine runs on saved data
- Monitoring dashboards
- **v1.1, one week after launch:** Replay Playground for practice on past data

**Not in v1**

- Real-money trading (never)
- Stocks, derivatives, and other exchanges (Kraken as a second venue is v2)
- Running user-submitted code on SimEx servers
- A market-impact model (big orders moving the price) — v2
- Playground scores on the live leaderboard (never; see Replay Playground)
- Strategy marketplace, social features, copy-trading, mobile app

## System architecture

Market data enters once through the ingestor; agents never connect to Coinbase directly. Orders and
fills pass through the gateway, every fill lands in the ledger exactly once, and the evaluator scores
agents from the ledger. Prometheus and Grafana watch consumer lag, gap recoveries and tick-to-fill
latency. See the diagram in the [README](../README.md#how-it-works).

### Components

| Group | Parts |
| --- | --- |
| Market data | Coinbase ingestor with gap resync, Kafka topics, order book builder, recorder |
| Exchange core | Order gateway, matching engine and fill simulator, stale-data guard, anti-cheat delay, ledger and reconciliation |
| Plug-ins | MCP server, Python SDK, framework adapters, accounts and API keys |
| Features and agents | Flink feature jobs, Redis, baseline agents, LLM reference agents with cost caps |
| Scoring and site | Evaluator, leaderboard, per-agent pages, reasoning log |
| Replay Playground | Window picker, speed control, run comparison, anonymous mode |
| Platform and operations | Terraform, docker-compose, CI/CD, Prometheus, Grafana, backups, load tests |

### Handling feed lag

Feed lag is harmless for LLM agents that take seconds to decide, if four rules hold:

1. Candles and features use the exchange's event timestamps, not arrival time.
2. Lag (exchange time vs. received time) is logged per message and shown on a dashboard.
3. The stale-data guard pauses fills and rejects orders while lag exceeds a threshold (start at 2
   seconds) or a gap is unrecovered.
4. Orders fill against the book at the moment they arrive, plus a fixed anti-cheat delay at least as
   long as feed lag. This closes latency arbitrage: a bot on a faster direct feed can't trade
   against SimEx's slightly stale book.

### Replay Playground

The playground runs saved market data through the same exchange engine, so agent code is identical
for practice and live.

- **Data:** SimEx's own recordings (full order book, realistic fills) plus Coinbase historical
  candles for longer history (labeled low-fidelity).
- **Controls:** pick a window, run at real time, 10x or full speed, rerun and compare runs side by
  side.
- **Anonymous mode:** hides dates and coin names and rescales prices, so a model can't recognize a
  period it saw in training.
- **Rule:** playground scores never appear on the live leaderboard. Only live forward-testing counts.

## Tech stack

About 70% of the stack is existing ground (Python, FastAPI, Kafka, Postgres, Redis, Iceberg, Docker,
MCP, an LLM gateway). The other 30% closes real gaps on purpose: Flink, Schema Registry,
property-based testing and Next.js.

| Component | Tools | Why |
| --- | --- | --- |
| Market data | Python websockets client, Apache Kafka (KRaft), Schema Registry with Avro, sortedcontainers for book levels | Schema evolution; fast book updates |
| Recorder + playground | PyArrow to Parquet on S3, PyIceberg tables, DuckDB for loading replay windows | A past day loads in seconds |
| Exchange core | Python asyncio (one writer per pair), FastAPI (REST + WebSocket), Pydantic, Python Decimal for all money | No race conditions; never floats for money |
| Ledger | PostgreSQL append-only events, asyncpg, transactional outbox to Kafka | Exactly-once fills, provable balances |
| Features | Apache Flink (Flink SQL / PyFlink), Redis | Event-time windows |
| Plug-ins | MCP Python SDK, Python client SDK on PyPI, hashed API keys, Redis rate limiting | Zero-code access for MCP clients |
| Agents | LLM gateway pattern (or LiteLLM), Anthropic / OpenAI / Gemini SDKs, Ollama | Cost caps per agent |
| Evaluation | Polars or pandas, NumPy, SciPy bootstrap tests | Significance vs baselines |
| Website | Next.js + TypeScript, Tailwind, TradingView Lightweight Charts (open source) | Public pages with trading-style charts |
| Monitoring | Prometheus, Grafana, OpenTelemetry, OpenLLMetry for agent LLM tracing | Lag, gaps, latency, LLM traces |
| Testing | pytest, Hypothesis property-based tests, testcontainers, Locust or k6 | Ledger invariants checked over thousands of random order sequences |
| Infra + delivery | Docker, docker-compose, Terraform, AWS EC2 + S3, GitHub Actions | One cheap host now, EKS-ready later |
| Project management | GitHub Projects, Issues, Milestones, decision records in `docs/adr` | Everything next to the code |

## Build plan

Week 2 is the first demo worth showing anyone; week 6 is public launch. Each phase ends in a gate
that must pass before the next phase starts.

| When | Phase | Gate |
| --- | --- | --- |
| Days 1–3 | Setup | Whole stack starts with one command |
| Week 1 | Live data in | 24h run with no unrecovered gaps; book matches Coinbase snapshots |
| Week 2 | Exchange core | Demo: Claude trades live via MCP; ledger reconciles to the cent |
| Week 3 | Plug-ins | An outside developer connects a bot using only the docs |
| Week 4 | Features and agents | All agents run 72h unattended within the LLM cost cap |
| Week 5 | Scoring and leaderboard | Public page shows live rankings and the reasoning behind each trade |
| Week 6 | Harden and launch | Deployed publicly; first outside agents on the leaderboard |
| Week 7 | Replay Playground (v1.1) | Agents replay a past window; scores stay off the live board |

**Days 1–3 · Setup**

- [ ] Repo, license, README skeleton, GitHub Actions
- [ ] docker-compose: Kafka, Postgres, Redis, Prometheus, Grafana
- [ ] Terraform for one EC2 host and an S3 bucket

**Week 1 · Live data in**

- [ ] Coinbase WebSocket client: market trades, level 2, heartbeats
- [ ] Sequence-gap detection and snapshot resync
- [ ] Order book builder per pair; spot-check against Coinbase
- [ ] Recorder to Parquet on S3, plus a replay reader

**Week 2 · Exchange core**

- [ ] Order model: market, limit, cancel, with idempotency keys
- [ ] Fill simulator: market orders walk the book; resting limit orders join the back of the queue
      and fill once live trades clear the volume ahead; maker/taker fees; delay
- [ ] Stale-data guard: pause fills and reject orders while lag exceeds the threshold or a gap is
      unrecovered
- [ ] Anti-cheat order delay, set from measured feed lag
- [ ] Ledger tables (Decimal amounts), balance projection, reconciliation job, Hypothesis invariant
      tests
- [ ] Minimal MCP server (quote, place order, portfolio); record the Claude demo

**Week 3 · Plug-ins**

- [ ] Full REST + WebSocket API with a live fills stream
- [ ] MCP tools for features, open orders, cancel, history; a required reasoning field on every order
- [ ] Python SDK and quickstart; accounts, API keys, rate and risk limits

**Week 4 · Features and agents**

- [ ] Flink jobs: 1m and 5m candles, VWAP, volatility, book imbalance into Redis
- [ ] Baselines: buy-and-hold, DCA, random, momentum
- [ ] Three LLM reference agents on a fixed cadence, cost cap enforced

**Week 5 · Scoring and leaderboard**

- [ ] Evaluator: mark-to-market, Sharpe, max drawdown, turnover, fee drag, significance test vs
      baselines
- [ ] Leaderboard and per-agent page with equity curve and reasoning log
- [ ] Fill-accuracy report: replayed fills vs real Coinbase trades

**Week 6 · Harden and launch**

- [ ] Load test; fill in the metrics table
- [ ] Grafana dashboards and alerts
- [ ] Public deploy; README with architecture and known limits; launch write-up
- [ ] Announce season 1 and invite outside agents

**Week 7 · Replay Playground (v1.1)**

- [ ] Window picker over recordings and Coinbase historical candles (candle runs labeled
      low-fidelity)
- [ ] Speed control: real time, 10x, full speed; deterministic reruns
- [ ] Side-by-side run comparison
- [ ] Anonymous mode: hide dates and coin names, rescale prices
- [ ] Keep playground scores separate from the live leaderboard; announce v1.1

## Project management

Run it as solo Scrumban: one-week sprints for rhythm, a Kanban board for flow, and the phase gates as
milestones.

- **Board (GitHub Projects):** Backlog → This week → In progress (max 2) → Testing → Done. The
  two-item limit stops half-finished work piling up.
- **Issues:** one day or less each, labeled by component; each phase is a milestone that closes only
  when its gate passes.
- **Monday (30 min):** pick the week's issues from the phase checklist.
- **Daily (5 min):** one dev-log line: what moved, what's blocked.
- **Friday (1 hr):** record a 1–2 minute demo video, then a short retro on what slowed you down.
- **End of week 1:** compare actual hours to plan and re-estimate the remaining weeks.

Rules that keep it shipping:

1. Thin end-to-end slice first: one trade from live data to the ledger by week 2.
2. Hardest risk first: fill simulator and ledger before the website.
3. New ideas go to a v2 column, never into this week.
4. Done means merged, tests passing, README updated, metric recorded.
5. Each big choice gets a short decision record in `docs/adr`.
6. Post the Friday demo publicly each week.

## Metrics to capture

Every number in the README comes from these measurements, taken from the running system. Targets get
set after the first baseline run, not before.

| Metric | How it's measured | Shows |
| --- | --- | --- |
| Market messages per second (sustained, peak) | Ingestor counters | Throughput |
| Feed lag p50 / p99 | Exchange timestamp vs received time, per message | Data freshness |
| Sequence gaps detected and recovered | Gap detector log | Reliability |
| Tick-to-fill latency p50 / p95 / p99 | Feed receive time to fill event time | Low latency |
| Orders per second and concurrent agents | Load test (Locust or k6) | Scale |
| Ledger reconciliation mismatches | Scheduled recompute vs ledger (goal: 0) | Correctness |
| Fill-model error vs real trades | Replay: simulated fill price vs actual Coinbase prints, in basis points | Realism |
| LLM cost per agent-day | Gateway logs | Cost discipline |
| Uptime over launch month | Health checks | Operations |
| External agents registered, weekly active | App database | Real users |

## Risks and mitigations

The biggest risk is an unrealistic fill model, because it would make every score meaningless; the
fill-accuracy check exists to catch it.

| Risk | Impact | Mitigation |
| --- | --- | --- |
| Fill model too optimistic | Scores mean nothing | Validate against real Coinbase trades in replay; cap order size against top-of-book depth; publish known limits |
| Latency arbitrage by bots on faster direct feeds | Free fake profit; leaderboard gamed | Anti-cheat order delay at least as long as feed lag; option to fill only against trades after arrival |
| Feed lags, drops messages or disconnects | Stale prices, corrupted book | Stale-data guard, sequence checks, snapshot resync, heartbeat channel, alert on gap |
| LLMs remember past periods from training | Playground scores look better than real skill | Playground kept off the leaderboard; anonymous mode; prefer windows after model cutoffs |
| LLM costs grow | Budget blown | Users bring their own keys; reference agents on a slow cadence; hard monthly cap |
| Scope creep | Never ships | v1 list frozen; new ideas go to a v2 column |
| Read as financial advice | Legal and reputational risk | Simulation-only, clear disclaimers, no real-money or copy-trading features |
| Abuse of the API | Outages, cost | API keys, rate limits, per-account caps |
| Single host fails | Downtime | State rebuilt from Kafka + ledger; daily backups |
| No one shows up | Weak portfolio story | Weekly seasons, a bot posting notable trades, framework adapters, weekly build-in-public posts |

## Open decisions

- [ ] Hours per week (sets the real timeline)
- [ ] Final project name
- [ ] Kafka or Redpanda
- [ ] Flink or Spark Structured Streaming
- [ ] Frontend: Next.js or Streamlit for v1
- [ ] Hosting: single EC2 host, a cheaper VPS, or EKS
- [ ] Which three LLMs run as reference agents, and the monthly LLM cap
- [ ] Launch channels (Bluesky, X, Hacker News, Medium)

## Sources

- [Coinbase Advanced Trade WebSocket guide](https://docs.cdp.coinbase.com/coinbase-business/advanced-trade-apis/guides/websocket)
  and [overview](https://docs.cdp.coinbase.com/coinbase-business/advanced-trade-apis/websocket/websocket-overview)
- [Binance HTTP 451 restricted-location error (ccxt issue)](https://github.com/ccxt/ccxt/issues/15872)
- [Alpha Arena coverage, Decrypt](https://decrypt.co/345006/ai-crypto-trading-showdown-deepseek-grok-winning-gemini-implodes?amp=1)
- [Open Finance Lab AgenticTrading](https://github.com/Open-Finance-Lab/AgenticTrading)
