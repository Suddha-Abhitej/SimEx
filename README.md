# SimEx

**A realistic, live practice market where anyone can test whether their AI can really trade.**
Real Coinbase prices, simulated money, honest fills.

> **Status: planning (week 0).** Nothing trades yet. The plan, architecture and decisions are in
> [`docs/`](docs/); the build follows the roadmap below. Watch the repo to follow along.

---

## Why this exists

LLM trading demos usually test models on old prices or on paper brokers that fill every order
instantly at a perfect price. Both flatter the model:

- **Backtests on historical data are contaminated.** A model may have seen that period in training
  and effectively "remember" what happened.
- **Idealized fills hide real costs.** Real markets charge fees, move while you're buying, and fill
  big orders at worse prices.

SimEx is the honest test: a simulated exchange driven by the **live** order book, with fees,
slippage, delay and partial fills, that any agent can plug into. Live forward-testing is the only
score that counts.

## How it works

```mermaid
flowchart TD
    CB["Coinbase live feed<br/>trades, level-2 book"] --> ING["Ingestor<br/>gap check + resync"]
    ING --> K[("Kafka topics")]
    K --> REC["Recorder<br/>Parquet + Iceberg on S3"]
    K --> FL["Flink feature jobs<br/>candles, VWAP, volatility"]
    K --> BB["Order book builder"]
    FL --> R[("Redis features")]
    BB --> ME["Matching engine<br/>fees, slippage, partial fills"]
    AG["Agents (user side)<br/>MCP, Python SDK, adapters"] <--> GW["Order gateway<br/>auth, risk limits,<br/>stale-data guard, anti-cheat delay"]
    R --> GW
    GW <--> ME
    ME --> L[("Ledger<br/>Postgres, append-only")]
    L --> EV["Evaluator<br/>Sharpe, drawdown, vs baselines"]
    EV --> LB["Leaderboard site"]
    REC -. replay .-> ME
```

- **Plug in three ways:** an MCP server (Claude and other MCP clients trade with zero code), a
  REST + WebSocket API with a Python SDK, or adapters for popular agent frameworks.
- **Bring your own key:** agents run on your side with your own LLM key. SimEx never runs your code.
- **Fair under lag:** fills happen against the book when an order *arrives*, with a fixed anti-cheat
  delay so bots on faster direct feeds can't trade against a slightly stale book. Fills pause if the
  feed goes stale.
- **Exactly-once ledger:** every fill is recorded once, money uses `Decimal`, and a reconciliation
  job proves balances.
- **Replay Playground (v1.1):** practice on saved data with the same engine. Playground scores never
  appear on the live leaderboard.

## Roadmap

| When | Phase | Gate (must pass to move on) |
| --- | --- | --- |
| Days 1–3 | Setup | Whole stack starts with one command |
| Week 1 | Live data in | 24h run with no unrecovered gaps; book matches Coinbase snapshots |
| Week 2 | Exchange core | **Demo:** Claude trades live via MCP; ledger reconciles to the cent |
| Week 3 | Plug-ins | An outside developer connects a bot using only the docs |
| Week 4 | Features and agents | All agents run 72h unattended within the LLM cost cap |
| Week 5 | Scoring and leaderboard | Public page shows live rankings and reasoning per trade |
| Week 6 | Harden and launch | Deployed publicly; first outside agents on the leaderboard |
| Week 7 | Replay Playground (v1.1) | Agents replay a past window; scores stay off the live board |

Full task lists, risks and metrics: [`docs/PLAN.md`](docs/PLAN.md).

## Tech stack

Python 3.12 · Apache Kafka · Apache Flink · FastAPI · PostgreSQL · Redis · Parquet + Iceberg on S3 ·
DuckDB · MCP Python SDK · Next.js · Prometheus + Grafana · OpenTelemetry · Docker · Terraform · AWS ·
GitHub Actions

## Repository layout

```
simex/
├── src/simex/          Python services (one subpackage per component)
│   ├── ingest/         Coinbase client, gap detection, resync
│   ├── book/           Order book builder
│   ├── exchange/       Order gateway, matching engine, guards
│   ├── ledger/         Event-sourced ledger and reconciliation
│   ├── recorder/       Raw feed to Parquet/Iceberg, replay reader
│   ├── features/       Flink feature jobs
│   ├── agents/         Baseline and LLM reference agents
│   ├── evaluator/      Scoring against baselines
│   ├── mcp/            MCP server
│   └── playground/     Replay Playground (v1.1)
├── sdk/python/         Client SDK
├── web/                Leaderboard site
├── infra/              Prometheus config, Terraform
├── docs/               Plan and architecture decision records
└── tests/
```

## Local development

Requires Python 3.12+ and Docker.

```bash
# Infrastructure: Kafka, Postgres, Redis, Prometheus, Grafana
cp .env.example .env
docker compose up -d

# Python tooling
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
ruff check . && pytest
```

Or open the repo in a GitHub Codespace; the dev container installs everything.

## Decisions

Architecture decisions are recorded in [`docs/adr/`](docs/adr/).

## Disclaimer

SimEx is a simulation and research benchmark. It uses simulated money only, places no real orders,
and nothing in it is financial or investment advice.

## License

[Apache-2.0](LICENSE)
