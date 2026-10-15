# SimEx: guidance for Claude Code

SimEx is a personal research tool for testing the owner's own AI traders against a live market:
live Gemini market data, simulated money, honest fills (ADR 0010). Read `docs/PLAN.md` for scope and
the build plan, and `docs/adr/` for decisions already made. Do not re-open an accepted ADR without
saying so explicitly.

## Hard rules

- **Money is `Decimal`, never float**, in the ledger, matching engine, fees and balances
  (ADR 0004). Serialize decimals as strings.
- **Simulation only.** Never add code that places real orders or moves real money (ADR 0002).
- **Only the ingestor talks to an exchange.** Every other component reads from Kafka and must not
  depend on which venue the data came from.
- **Personal use only (ADR 0010).** No public leaderboard, public sign-up, downloadable recordings,
  or public page showing market data or results derived from it. Any web UI is private.
- **Never commit market data**: no recordings, feed dumps or fixtures copied from a live feed. Test
  fixtures are hand-written or synthetic.
- **Coinbase data never feeds agents, the matching engine or scoring** (its terms forbid using it
  to benchmark AI agents). The Coinbase adapter is off by default and only for feed research.
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
- Market data: Gemini public WebSocket `wss://ws.gemini.com`, no auth. Pairs: BTCUSD, ETHUSD,
  SOLUSD. Venue adapters live in `src/simex/ingest/` and all publish the same envelope and topics
  (ADR 0007). Feeds can drop messages: always check the venue's sequence or update IDs and resync
  on gaps (ADR 0008).
- The Coinbase Advanced Trade adapter (`wss://advanced-trade-ws.coinbase.com`) stays for feed
  research only (see hard rules).

## Commands

```bash
pip install -e ".[dev]"      # install with dev tools
ruff check . && ruff format --check . && pytest
docker compose up -d         # local infrastructure
python tools/feed_probe.py --seconds 300   # measure live feed volume and lag
```
