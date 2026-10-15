# SimEx: guidance for Claude Code

SimEx is a realistic, live practice market where anyone can test whether their AI can really
trade: live Coinbase market data, simulated money, honest fills. Read `docs/PLAN.md` for scope and
the build plan, and `docs/adr/` for decisions already made. Do not re-open an accepted ADR without
saying so explicitly.

## Hard rules

- **Money is `Decimal`, never float**, in the ledger, matching engine, fees and balances
  (ADR 0004). Serialize decimals as strings.
- **Simulation only.** Never add code that places real orders or moves real money (ADR 0002).
- **Only the ingestor talks to Coinbase.** Every other component reads from Kafka.
- **One writer per trading pair** in the matching engine; no shared mutable state across pairs.
- **Never commit secrets**: no `.env`, keys (`*.key`, `*.pem`), tokens or passwords. Use
  `.env.example` for placeholders.
- **No AI attribution** in commits or PR descriptions: no `Co-Authored-By` trailers, no
  "Generated with" footers.

## Workflow

- Never commit to `main`. Work on a branch named `<type>/<short-name>` (for example
  `feat/coinbase-ingestor`, `infra/compose-limits`) and open a pull request.
- Reference the GitHub issue in the PR (`Closes #N`).
- Keep PRs small: one issue, one PR.
- Before opening a PR, run `ruff check .`, `ruff format --check .` and `pytest`; all must pass.
- A significant design choice gets a short ADR in `docs/adr/` using `template.md`, and a row in
  `docs/adr/README.md`.
- If a task is ambiguous, ask before building.

## Stack and environment

- Python 3.12+, asyncio. Package code lives in `src/simex/<component>/`, tests in `tests/`.
- Infrastructure runs in Docker Compose (`compose.yaml`): Kafka in KRaft mode, Postgres, Valkey
  (Redis-compatible), Prometheus, Grafana.
- Production target: Oracle Cloud Always Free, **ARM64 (aarch64), 2 cores, 12 GB RAM**. Every image
  must support `linux/arm64`, and every service needs a memory limit. Development also happens on
  x86 Windows laptops, so keep everything multi-arch.
- Market data: Coinbase Advanced Trade WebSocket `wss://advanced-trade-ws.coinbase.com`, public
  channels `level2`, `market_trades`, `heartbeats`, no auth. Pairs: BTC-USD, ETH-USD, SOL-USD.
  The feed can drop messages: always check `sequence_num` and resync on gaps.

## Commands

```bash
pip install -e ".[dev]"      # install with dev tools
ruff check . && ruff format --check . && pytest
docker compose up -d         # local infrastructure
python tools/feed_probe.py --seconds 300   # measure live feed volume and lag
```
