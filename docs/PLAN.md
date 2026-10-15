# SimEx project plan

Last updated: 2026-10-14

## Pitch and goals

SimEx is a personal, live practice market for testing whether an AI can really trade: live Gemini
prices, fake money, honest fills.

> **Scope change (2026-10-14, ADR 0010):** exchange data terms allow personal use only, and
> Coinbase's forbid using its data to benchmark AI agents. SimEx is now a private research tool for
> its owner on Gemini data. The code stays public; no market data, recordings or leaderboards are
> published.

- **User goal:** plug in my own LLM agents or bots (MCP, API, or a framework adapter), trade against
  the live market with real fees, slippage and delay, and get a fair score against baselines.
- **Portfolio goal:** open-source code and a running private system that show streaming
  correctness, an exchange-grade ledger, pluggable market sources, and rigorous LLM evaluation.
- **Why it's different:** other agent platforms rely on a third-party paper broker or testnet with
  idealized fills. SimEx is the realistic exchange layer any framework plugs into.
- **Headline insight:** live forward-testing is the only clean way to judge an LLM trader. Backtests
  on old data are contaminated, because the model may have seen that period in training.

## Assumptions and constraints

The six-week timeline assumes 15 to 20 hours a week, a $0 hosting budget, and simulation-only use.
Change any of these and the timeline moves.

| Assumption | v1 value | Why |
| --- | --- | --- |
| Time | 15–20 hrs/week, about 6 weeks | Fits alongside other commitments |
| Budget | **$0/month**: Oracle Always Free + free tiers; LLM spend hard-capped | Licences that allow public redistribution are sales-quoted, about $1,750+/month |
| Hosting | Oracle Cloud Always Free Ampere A1 (ARM64, 2 cores, 12 GB, 200 GB disk) for production; home server (Ryzen 7, 16 GB, 2 TB) as archive and lab | See ADR 0006 |
| Market venue | Gemini public WebSocket (`wss://ws.gemini.com`), no auth; Coinbase adapter kept for feed research only | Gemini allows personal use with no AI clause (ADR 0010); Binance.com blocks US IPs |
| Pairs | BTCUSD, ETHUSD, SOLUSD | Busy around the clock |
| Audience | The owner only; any web UI private behind Cloudflare Access | Exchange terms allow personal use only |
| Who runs agents | The owner, outside SimEx, with their own key | SimEx never executes agent code |
| Money | Simulation only: no real orders, no copy-trading | Keeps it a benchmark, not financial advice |
| Language | Python first | Strongest existing skills |
| Infrastructure memory | Kafka 1.25 GB, Postgres 1 GB, Valkey 256 MB, Prometheus 512 MB (Grafana 256 MB, opt-in) | About 3 GB of a 12 GB ARM64 host, leaving room for the services |

## Scope

**In v1**

- Gemini as the live venue: trades and level-2 order book for BTCUSD, ETHUSD and SOLUSD, with gap
  detection and resync, behind a pluggable venue-adapter interface
- Matching engine: market, limit and cancel orders; fees; simulated delay; partial fills
- Stale-data guard: fills pause and orders are rejected when the feed lags or drops messages
- Anti-cheat order delay, at least as long as feed lag, so fast outside feeds can't trade against a
  stale book
- Event-sourced ledger with exactly-once fills and an automatic reconciliation check
- REST + WebSocket API, an MCP server, and a Python SDK
- API keys and per-agent risk limits (one owner, several agents)
- Streaming features: candles, VWAP, volatility, order-book imbalance
- Four baseline agents (buy-and-hold, DCA, random, simple momentum) and three LLM reference agents
- Private scoreboard, per-agent equity curve, and a reasoning log for every decision
- Recorder and replay mode, so the same engine runs on saved data
- Monitoring dashboards
- **v1.1, one week after deploy:** Replay Playground for practice on past data

**Not in v1**

- Real-money trading (never)
- Public leaderboard, public sign-up, or downloadable recordings (needs a redistribution licence or
  a synthetic market; see ADR 0010)
- Stocks, derivatives, and more venues (a synthetic market source is a v2 option for public demos)
- Running user-submitted code on SimEx servers
- A market-impact model (big orders moving the price) — v2
- Playground scores mixed with live scores (never; see Replay Playground)
- Strategy marketplace, social features, copy-trading, mobile app

## System architecture

Market data enters once through the ingestor; agents never connect to the exchange directly. Orders and
fills pass through the gateway, every fill lands in the ledger exactly once, and the evaluator scores
agents from the ledger. Prometheus and Grafana watch consumer lag, gap recoveries and tick-to-fill
latency. See the diagram in the [README](../README.md#how-it-works).

### Components

| Group | Parts |
| --- | --- |
| Market data | Ingestor with venue adapters (Gemini live, Coinbase research) and gap resync, Kafka topics, order book builder, recorder |
| Exchange core | Order gateway, matching engine and fill simulator, stale-data guard, anti-cheat delay, ledger and reconciliation |
| Plug-ins | MCP server, Python SDK, framework adapters, API keys |
| Features and agents | Flink feature jobs, Valkey, baseline agents, LLM reference agents with cost caps |
| Scoring and dashboard | Evaluator, private scoreboard, per-agent pages, reasoning log |
| Replay Playground | Window picker, speed control, run comparison, anonymous mode |
| Platform and operations | Docker Compose, Cloudflare Tunnel, Terraform (Cloudflare, Azure), CI/CD, Prometheus, Grafana, backups, load tests |

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

- **Data:** SimEx's own private Gemini recordings (full order book, realistic fills), plus Gemini
  historical candles for longer history (labeled low-fidelity). Never published.
- **Controls:** pick a window, run at real time, 10x or full speed, rerun and compare runs side by
  side.
- **Anonymous mode:** hides dates and coin names and rescales prices, so a model can't recognize a
  period it saw in training.
- **Rule:** playground scores are kept separate from live scores. Only live forward-testing counts.

## Tech stack

About 70% of the stack is existing ground (Python, FastAPI, Kafka, Postgres, Valkey, Iceberg, Docker,
MCP, an LLM gateway). The other 30% closes real gaps on purpose: Flink, Schema Registry,
property-based testing and Next.js.

| Component | Tools | Why |
| --- | --- | --- |
| Market data | Python websockets client, Apache Kafka (KRaft), Schema Registry with Avro, sortedcontainers for book levels | Schema evolution; fast book updates |
| Recorder + playground | PyArrow to zstd Parquet on local disk (~30 days on Oracle), nightly sync to the home server's 2 TB archive; PyIceberg, DuckDB for replay | Months of history at $0 |
| Exchange core | Python asyncio (one writer per pair), FastAPI (REST + WebSocket), Pydantic, Python Decimal for all money | No race conditions; never floats for money |
| Ledger | PostgreSQL append-only events, asyncpg, transactional outbox to Kafka | Exactly-once fills, provable balances |
| Features | Apache Flink (Flink SQL / PyFlink) or Python jobs first, Valkey | Event-time windows |
| Plug-ins | MCP Python SDK, Python client SDK on PyPI, hashed API keys, Valkey rate limiting | Zero-code access for MCP clients |
| Agents | LLM gateway pattern (or LiteLLM), Anthropic / OpenAI / Gemini SDKs, Ollama | Cost caps per agent |
| Evaluation | Polars or pandas, NumPy, SciPy bootstrap tests | Significance vs baselines |
| Dashboard | Next.js + TypeScript (or Streamlit), TradingView Lightweight Charts (open source), behind Cloudflare Access | Private trading-style charts |
| Monitoring | Prometheus, Grafana, OpenTelemetry, OpenLLMetry for agent LLM tracing | Lag, gaps, latency, LLM traces |
| Testing | pytest, Hypothesis property-based tests, testcontainers, Locust or k6 | Ledger invariants checked over thousands of random order sequences |
| Infra + delivery | Docker Compose on Oracle ARM64, Cloudflare Tunnel, Terraform, multi-arch images via GitHub Actions; website on Azure Static Web Apps | $0, portable to any host |
| Project management | GitHub Projects, Issues, Milestones, decision records in `docs/adr` | Everything next to the code |

## Build plan

Week 2 is the first working demo; week 6 is an unattended private deployment. Each phase ends in a gate
that must pass before the next phase starts.

| When | Phase | Gate |
| --- | --- | --- |
| Days 1–3 | Setup | Whole stack starts with one command |
| Week 1 | Live data in | 24h run on Gemini with no unrecovered gaps; book matches fresh snapshots |
| Week 2 | Exchange core | Demo: Claude trades live via MCP; ledger reconciles to the cent |
| Week 3 | Plug-ins | A new agent connects using only the docs |
| Week 4 | Features and agents | All agents run 72h unattended within the LLM cost cap |
| Week 5 | Scoring | Private scoreboard shows live rankings and the reasoning behind each trade |
| Week 6 | Harden and deploy | Runs 7 days unattended on Oracle; write-up of architecture and system metrics |
| Week 7 | Replay Playground (v1.1) | Agents replay a past window; scores stay separate from live |

**Days 1–3 · Setup**

- [ ] Repo, license, README skeleton, GitHub Actions
- [x] docker-compose: Kafka, Postgres, Valkey, Prometheus, Grafana (#2)
- [ ] Oracle Always Free server, data disk, Docker; Cloudflare Tunnel and domain (#1)

**Week 1 · Live data in**

- [x] Coinbase WebSocket client: market trades, level 2, heartbeats (#3; now research only)
- [x] Sequence-gap detection and snapshot resync (#4)
- [ ] Venue-adapter interface and Gemini adapter: depth diffs, trades, gap detection on update IDs,
      resync (ADR 0010)
- [ ] Order book builder per pair, venue-independent; spot-check against fresh Gemini snapshots
- [ ] Recorder to zstd Parquet on local disk, plus a replay reader and nightly sync to the home server

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
- [ ] Python SDK and quickstart; API keys and per-agent risk limits

**Week 4 · Features and agents**

- [ ] Flink jobs: 1m and 5m candles, VWAP, volatility, book imbalance into Valkey
- [ ] Baselines: buy-and-hold, DCA, random, momentum
- [ ] Three LLM reference agents on a fixed cadence, cost cap enforced

**Week 5 · Scoring**

- [ ] Evaluator: mark-to-market, Sharpe, max drawdown, turnover, fee drag, significance test vs
      baselines
- [ ] Private scoreboard and per-agent page with equity curve and reasoning log, behind Cloudflare
      Access
- [ ] Fill-accuracy report: replayed fills vs real Gemini trades

**Week 6 · Harden and deploy**

- [ ] Load test; fill in the metrics table
- [ ] Grafana dashboards and alerts
- [ ] Private deploy on Oracle; 7-day unattended run
- [ ] README with architecture and known limits; write-up with system metrics only (no market data)

**Week 7 · Replay Playground (v1.1)**

- [ ] Window picker over private recordings and Gemini historical candles (candle runs labeled
      low-fidelity)
- [ ] Speed control: real time, 10x, full speed; deterministic reruns
- [ ] Side-by-side run comparison
- [ ] Anonymous mode: hide dates and coin names, rescale prices
- [ ] Keep playground scores separate from live scores

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
6. Friday demos shown publicly show architecture and system metrics, not market data or results.

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
| Fill-model error vs real trades | Replay: simulated fill price vs actual Gemini prints, in basis points | Realism |
| LLM cost per agent-day | Gateway logs | Cost discipline |
| Uptime over the first month | Health checks | Operations |
| Agents run, agent-days completed | App database | Evaluation coverage |

## Risks and mitigations

The biggest risk is an unrealistic fill model, because it would make every score meaningless; the
fill-accuracy check exists to catch it.

| Risk | Impact | Mitigation |
| --- | --- | --- |
| Fill model too optimistic | Scores mean nothing | Validate against real Gemini trades in replay; cap order size against top-of-book depth; document known limits |
| Latency arbitrage by bots on faster direct feeds | Free fake profit; scores inflated | Anti-cheat order delay at least as long as feed lag; option to fill only against trades after arrival |
| Feed lags, drops messages or disconnects | Stale prices, corrupted book | Stale-data guard, sequence checks, snapshot resync, heartbeat channel, alert on gap |
| LLMs remember past periods from training | Playground scores look better than real skill | Playground scores kept separate; anonymous mode; prefer windows after model cutoffs |
| LLM costs grow | Budget blown | Reference agents on a slow cadence; hard monthly cap; local models via Ollama |
| Scope creep | Never ships | v1 list frozen; new ideas go to a v2 column |
| Read as financial advice | Legal and reputational risk | Simulation-only, clear disclaimers, no real-money or copy-trading features |
| API reachable by others | Data exposed against exchange terms | Ports on 127.0.0.1; Cloudflare Access in front of anything served; API keys |
| Single host fails | Downtime | State rebuilt from Kafka + ledger; nightly ledger backup off the box; home server as warm standby |
| Oracle changes or reclaims the free tier | Server lost or shrunk | Everything in Docker Compose; move to the home server in about an hour |
| Exchange terms change | Live source lost | Re-read terms before new uses; pluggable sources; a synthetic market as fallback |
| No public users | Weaker portfolio story | Open-source code, measured system metrics, licensing review documented in ADR 0010 |

## Open decisions

- [ ] Hours per week (sets the real timeline)
- [ ] Final project name
- [x] Message bus: Kafka (KRaft)
- [ ] Flink or Spark Structured Streaming
- [ ] Dashboard: Next.js or Streamlit for v1
- [x] Audience and venue: personal use on Gemini data (ADR 0010)
- [x] Hosting: Oracle Always Free + home server archive, $0 (ADR 0006)
- [ ] Which three LLMs run as reference agents, and the monthly LLM cap

## Sources

- [Gemini WebSocket stream reference](https://developer.gemini.com/websocket/streams)
- [Coinbase Advanced Trade WebSocket guide](https://docs.cdp.coinbase.com/coinbase-business/advanced-trade-apis/guides/websocket)
  and [overview](https://docs.cdp.coinbase.com/coinbase-business/advanced-trade-apis/websocket/websocket-overview)
- [Binance HTTP 451 restricted-location error (ccxt issue)](https://github.com/ccxt/ccxt/issues/15872)
- [Alpha Arena coverage, Decrypt](https://decrypt.co/345006/ai-crypto-trading-showdown-deepseek-grok-winning-gemini-implodes?amp=1)
- [Open Finance Lab AgenticTrading](https://github.com/Open-Finance-Lab/AgenticTrading)
