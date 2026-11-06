# SimEx system dashboards

Start the optional monitoring UI with:

```sh
docker compose --profile monitoring up -d prometheus grafana
```

Open <http://127.0.0.1:3000> and find the **SimEx** folder. Use the local Grafana admin
credentials; set `GRAFANA_ADMIN_PASSWORD` before the first start. The existing data volume
retains Grafana's account settings. All service ports remain on loopback.

The Prometheus datasource, file provider and four dashboards are provisioned from the three
read-only mounts in [compose.yaml](../../compose.yaml). Stable UIDs keep links reproducible.
Edit the files and submit a PR; UI edits cannot be saved over provisioned dashboards. Removing
a dashboard file removes its provisioned dashboard on the next provider scan. Provisioning
files use JSON, a YAML subset, so tests can parse them without an extra dependency. Grafana's
[provisioning documentation](https://grafana.com/docs/grafana/latest/administration/provisioning/)
describes the provider behavior.

| Dashboard | Question |
| --- | --- |
| Feed health | Is the feed fresh, are gaps recovering, and are books passing audits? |
| Exchange | Is the gate open, and why is the writer rejecting orders? |
| Ledger | Are writes committing and reconciliation succeeding? |
| Recorder | Is the recorder keeping up and publishing its hourly files? |

The existing [Prometheus configuration](../prometheus/prometheus.yml) scrapes the host's ingestor,
book, recorder and exchange on ports 9101–9104. Grafana talks to Prometheus inside Compose.
Dashboards do not start those application processes. Unreachable scrape targets and absent series
remain visible as unavailable; they are not converted to healthy zeroes. Panels use the fixed
Gemini venue where a venue label exists. Histogram quantiles are bucket estimates, with counts
provided for interpreting ledger commit latency. Rate panels need enough scrapes to form a rate.

All 11 current [alert rules](../prometheus/rules/) have corresponding panels. **Prometheus owns
alerting; Grafana only visualises.** No Grafana alert definitions or duplicate alerting rules
are provisioned.

Submitted/accepted counters and continuous tick-to-fill latency are missing instrumentation,
tracked in [#111](https://github.com/Suddha-Abhitej/SimEx/issues/111). Writer rejection rates have
no submission denominator and exclude request-validation refusals returned before the writer.
Journal eviction can also undercount them; the exchange dashboard exposes that counter. Recorder
unread-offset lag, exchange caught-up flags and watermark lag are distinct measurements. The
exchange and book do not export numeric unread-offset lag.

Absolute feed lag requires a synchronized host clock; persistent negative lag is a clock-skew
warning ([#74](https://github.com/Suddha-Abhitej/SimEx/issues/74)). Recorder publication age can
normally approach an hour: files close hourly, and a clean shutdown also publishes partial files.
Neither quiet trades nor publication age alone should be read as recorder failure.

These dashboards contain system metrics only: no prices, equity or agent results
([ADR 0010](../../docs/adr/0010-personal-use-and-gemini-market-data.md)). They are private operational
tools; a system-panel screenshot contains none of the market data a scoring screenshot would.

[Dashboard tests](../../tests/test_grafana.py) check exported metric selectors, datasource/mount
contracts, alert-panel coverage and the absence of Grafana alert definitions. CI additionally
passes every dashboard expression through promtool, validates monitoring-profile Compose config,
and runs the existing Prometheus rule tests. The application and trading path are unchanged.
