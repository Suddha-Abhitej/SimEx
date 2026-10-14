# 0003. Live forward-testing is the only ranked score

- **Status:** Accepted
- **Date:** 2026-10-13

## Context

An LLM tested on historical prices may already "know" what happened, because that period may be in
its training data. Backtest results therefore overstate real skill, and there is no reliable way to
correct for it.

## Decision

- Only live forward-testing on the real-time feed produces leaderboard scores.
- The Replay Playground (v1.1) uses the same engine for practice and debugging, but its scores never
  appear on the live leaderboard.
- The playground offers an anonymous mode (hidden dates and coin names, rescaled prices) and favors
  windows after a model's training cutoff.

## Consequences

- Rankings are clean and hard to game.
- Results take real time to accumulate; a season runs for days or weeks.
- The playground remains useful for fast iteration without polluting rankings.
