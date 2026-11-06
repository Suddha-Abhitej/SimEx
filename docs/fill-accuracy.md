# Fill-accuracy history

This is the local maker-realism check defined in [ADR 0014 §B](adr/0014-order-model-and-fill-model.md#how-we-know-the-fill-model-is-honest-the-week-5-fill-accuracy-report).
It compares simulated maker fills with real Gemini prints' makers at the same side and price,
using markouts at 1, 10 and 60 seconds. It validates **fill quality, not fill frequency**: the
frequency assumption remains unverified without a real account's fill history.

**Aggregates only belong in this document:** counts, basis points, percentages and verdicts, as
already recorded in ADR 0014. Never add prices, individual fill timestamps, orders, feed excerpts
or recordings. [ADR 0010](adr/0010-personal-use-and-gemini-market-data.md) applies. Keep exported JSON
local under the gitignored `data/` directory; there is no upload or public report page.

## Run it

From the repository root, with Python 3.12+ and the package installed:

```bash
pip install -e ".[dev]"

# Smoke test: the latest complete published hour.
python tools/fill_replay_check.py

# A real check: 24 hours or more, with a local summary for later comparison.
python tools/fill_replay_check.py --last-hours 24 --json data/fill-accuracy/latest.json

# Preserve a separate summary for each measurement instead of overwriting latest.json.
python tools/fill_replay_check.py --last-hours 28 --json data/fill-accuracy/run-001.json

# Existing explicit windows, including fractional hours, still work.
python tools/fill_replay_check.py --root data/recordings --start 2026-10-15T16:00 --hours 28
```

`--start` accepts ISO times; an omitted timezone means UTC. With `--start`, `--hours` defaults to
one. `--hours` without `--start` is an error, and `--start` cannot be combined with `--last-hours`.
`--every` (default 60 seconds), `--lifetime` (300 seconds) and `--blackout` (0 seconds) keep their
existing meanings. All four attribution runs consume the same replay.

Automatic discovery considers published Gemini level2 parts for supported pairs. It ignores
temporary files, other venues/channels and the current or future UTC hour. A clean recorder
shutdown publishes open parts early, so a `.parquet` file alone does not prove its hour has ended
([ADR 0013](adr/0013-recorder-parquet-layout-exactly-once-retention.md)).

`--last-hours N` selects consecutive hours ending at the latest eligible published hour. If an
hour is missing, it fails with the missing hours instead of stitching disjoint periods together
or silently choosing an older window. No complete hour yet means wait for recorder publication.
A trade file is not required in every hour: silence is legitimate. Published hour availability
does not establish uninterrupted feed coverage or data for every pair; inspect the reported
counts, warm-up/resync/stale activity and matched samples. Explicit windows retain their original
selection behavior.

## Sample size and interpretation

The single-hour default is a **pipeline smoke test**, not an expectation of a judgeable finding.
The 28-hour measurement in ADR 0014 recorded 722 matched BTC fills, 552 ETH and 723 SOL:
about 26, 20 and 26 per hour respectively. Against `MIN_MATCHED = 20` at each horizon, an hour
will routinely have too few matched fills, especially with recording gaps or sparse trading.
Use `--last-hours 24` or more for a real check; many hours improve the chance of enough samples
but do not guarantee it.

A mean difference is **simulated maker markout minus real maker markout**, in basis points:

- Positive: the simulator gave its maker a better outcome than the matched real makers.
- Negative: the simulated maker did worse, the intended pessimistic direction.
- Above **+1 bp** at a horizon with **at least 20** matched fills fails the default run's maker
  comparison. Exactly +1 bp passes. Below 20, the mean is reported as **too few to judge**.
- With no matches, there is no mean. JSON uses `null`, never a fabricated 0.00 bp.

If every pair lacks a judgeable default-run comparison, one additional line explains that the
window was too short to judge and suggests a longer `--last-hours`. It does not change the exit
status. `PASS` means no existing failure condition fired; it does not make an undersampled run
conclusive. A longer window is useful, but missing or unmatched data can still limit the result.

The four runs are proportional/print (default), pessimistic/print, optimistic/print, and
proportional/fill. **Only the default maker comparison and default book-cross guard affect the
verdict; money creation/destruction or negative balances fail any run.** The fill bracket is
intentionally optimistic and may look better than real. Its JSON comparison is diagnostic and
must not cause an overall failure by design. The text report retains the existing counts,
percentages, default markouts and PASS/FAIL content, with the short-window notice when needed.

## Local JSON and drift

`--json PATH` adds one summary object without replacing text output. It is written atomically
after a completed measurement, including a completed FAIL. An unavailable window, replay failure
or journal eviction does not publish a partial measurement. A failed write exits nonzero; any
previous summary remains in place, so always check the command's exit status before using a file.

Schema version 1 contains:

- `window`: inclusive UTC start and exclusive end; `record_count` counts replayed records.
- `policy`: horizons, minimum matched count, optimism threshold, fill-within horizons, probe
  timing/blackout, default attribution/book-cross mode, and effective per-run configuration.
  Each run records maker/taker bps, fee increment and order delay; each pair records market band,
  attribution, book-cross mode, match window, depletion age and lag-baseline setting.
- `runs`: per-run money problems and per-pair aggregate counts and `maker_markouts`. Each horizon
  records matched/unmatched counts, mean difference in bps and `pass`, `fail`, `too_few_to_judge`
  or `no_matched_fills`. `markouts_affect_verdict` identifies the default run's comparisons.
- `verdict`, `problems` and `insufficient_samples`: the existing overall result plus its sample
  limitation, independent of diagnostic bracket results.

Counts are integers. Decimals, including bps, percentages and aggregate durations, are strings;
JSON retains the full Decimal mean while text displays the existing two decimal places. Internal
price-keyed markouts, individual events, balances and identifiers are never serialized.

**Policy blocks must match before comparing results as the same experiment.** Different fees,
bands, delay, attribution or thresholds describe different experiments even with identical column
headings. The policy is read from the actual writer and engine objects, not guessed from environment
variables that the tool may override. No git revision or clean checkout is required to capture it.
Keep local snapshots with distinct filenames, inspect their policy blocks and matched counts, then
add only the permitted aggregates to the history below. CI exercises synthetic tests; it does not
run this measurement on private recordings.

## Recorded results

Source: [ADR 0014, Measured](adr/0014-order-model-and-fill-model.md#measured), dated 2026-10-19.
These are the already recorded default-run aggregates, not a new run of local recordings.
The matched counts are reproduced as ADR 0014 reports them: it does not give separate counts
for each horizon. PASS is the recorded overall verdict, repeated alongside each pair's metrics.

| Window (UTC) | Pair | Run | Matched fills | Mean 1 s | Mean 10 s | Mean 60 s | Recorded verdict |
| --- | --- | --- | ---: | ---: | ---: | ---: | --- |
| 2026-10-15 16:00 – 2026-10-16 20:00 (28 h) | BTC-USD | proportional/print | 722 | −1.32 bp | −1.34 bp | −0.98 bp | PASS |
| 2026-10-15 16:00 – 2026-10-16 20:00 (28 h) | ETH-USD | proportional/print | 552 | −4.12 bp | −4.26 bp | −4.12 bp | PASS |
| 2026-10-15 16:00 – 2026-10-16 20:00 (28 h) | SOL-USD | proportional/print | 723 | −4.71 bp | −4.62 bp | −4.97 bp | PASS |

ADR 0014 also records a **superseded book-cross experiment** on 2026-10-16 12:00–13:00 UTC:
book crosses supplied 97–100% of maker fills, and 87–92% of passive probes filled within a minute.
That evidence led to changing the cross rule. It is historical context, not a comparable run under
the current policy; the ADR supplies no matched markout counts or means for that window to invent
here. Likewise, the other attribution runs' historical fill-rate brackets do not supply missing
per-run markout means.
