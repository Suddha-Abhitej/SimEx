# Private deployment and operation

SimEx runs for its owner only, with simulated money. Start with SSH forwarding; Cloudflare
Tunnel **plus Access** is an alternative below. Neither path requires opening an application
port to the Internet. This is an operating recipe, not a claim that Oracle capacity has been
measured; see the [known limits](../README.md#known-limits).

## Reach it through SSH first

On the server, keep every binding in [compose.yaml](../compose.yaml) at `127.0.0.1` and the
private API on its fixed loopback binding. In the cloud security list and host firewall, permit
SSH only from the owner's administrative source addresses; do not open service ports, including
IPv6 equivalents. No public ingress to ports 3000, 5432, 6379, 8765, 8770, 9090, 9092 or 9101–9104.
Examples use the default infrastructure ports and API port 8770; adjust forwarding destinations
and firewall coverage if you configure other ports.

From the owner's laptop, using the server's SSH account/address and verified host key:

```bash
ssh -N -T -o ExitOnForwardFailure=yes -o ServerAliveInterval=30 \
  -o ServerAliveCountMax=3 \
  -L 127.0.0.1:8770:127.0.0.1:8770 \
  -L 127.0.0.1:3000:127.0.0.1:3000 \
  -L 127.0.0.1:9090:127.0.0.1:9090 simex@host.example.com
```

These are [OpenSSH local forwards](https://man.openbsd.org/ssh#L): each destination is on the
server, while the listener is restricted to the laptop's loopback. Omit unneeded forwards;
choose different local ports if occupied and adjust the bot's URL/port accordingly. Keep this
terminal running. Open Grafana at <http://127.0.0.1:3000> using its private login, or Prometheus
at <http://127.0.0.1:9090>. Grafana must be started separately as described below.

Bots use `http://127.0.0.1:8770` through the forward, still supplying `EXCHANGE_API_TOKEN`.
SSH access does not replace API authentication. Keep the unauthenticated liveness endpoint
`/v1/health` inside the same private access boundary; it does not prove the trading gate is open.
Do not forward Kafka, Postgres or Valkey for routine bot operation.

## Prepare the host and configuration

The [ADR 0006](adr/0006-zero-cost-hosting-oracle-plus-home-archive.md) target is Ubuntu 24.04
on Oracle Ampere A1 **ARM64, 2 OCPU, 12 GB RAM**, with a 50 GB boot and 150 GB data disk.
Check the allocation and Always Free eligibility in your tenancy against Oracle's
[current allowance](https://docs.oracle.com/en-us/iaas/Content/FreeTier/freetier_topic-Always_Free_Resources.htm).
An allowance and container caps are not demonstrated exchange capacity.

Install Python 3.12+, Git and [Docker Engine with the Compose plugin](https://docs.docker.com/engine/install/ubuntu/)
for ARM64. The existing Compose image tags support ARM64; keep the pinned tags and memory
limits. Examples below assume an owner-controlled Unix account `simex` with a writable checkout
at `/srv/simex`, Docker access and a mounted persistent data disk. Substitute your actual paths
and user. Membership granting Docker access is privileged; it is not a sandbox for bot code.

```bash
git clone https://github.com/Suddha-Abhitej/SimEx.git /srv/simex
cd /srv/simex
python3 -m venv .venv
.venv/bin/python -m pip install -e '.[dev]'
umask 077
cp .env.example .env
chmod 600 .env
```

Edit your private `.env` before starting services. Replace Postgres/Grafana example passwords
and choose an API token; keep credentials out of git and shared logs. API tokens belong in
Authorization headers, never request URLs. Use matching
Postgres credentials in `EXCHANGE_LEDGER_DSN` (URL-encode reserved password characters).
Existing Postgres/Grafana volumes retain their credentials: changing the initial password
environment variables is not a password rotation for an existing volume.

For a single-product deployment, these settings supplement the infrastructure settings copied
from [.env.example](../.env.example). **Replace all credential and cap placeholders locally.**
Caps are your policy; no values below claim a measured safe strategy size.

```dotenv
SIMEX_VENUE=gemini
SIMEX_COINBASE_RESEARCH=0
SIMEX_PAIRS=BTC-USD
SIMEX_INGEST_DROP_EVERY=0
EXCHANGE_ACCOUNT_ID=owner
EXCHANGE_INITIAL_USD=100000
EXCHANGE_LEDGER_DSN=postgresql://simex:<url-encoded-private-password>@127.0.0.1:5432/simex
EXCHANGE_API_PORT=8770
EXCHANGE_API_TOKEN='<choose-your-private-token>'
EXCHANGE_MAX_QUANTITY_BTC_USD=<positive-decimal-base-quantity>
EXCHANGE_MAX_NOTIONAL_BTC_USD=<positive-decimal-quote-notional>
EXCHANGE_MAX_POSITION_BTC_USD=<positive-decimal-absolute-base-position>
RECORDER_REQUIRE_ARCHIVED=1
```

Every enabled product needs explicit **quantity, notional and position** limits: the runtime
has no fallback for any of the three. With the default three products, also set the ETH_USD and
SOL_USD versions of all three variables. Startup fails if any are missing. These account risk
caps are independent of CPU/memory caps or measured request throughput
([ADR 0022](adr/0022-account-order-and-position-risk-limits.md)). Leave MCP disabled unless
needed; if enabled it stays local and must use the same private access boundary.

The ledger seeds a new account once. Restarting, rebuilding after Kafka failure or changing
`EXCHANGE_INITIAL_USD` does **not** top up an existing account. Restore the intended ledger and
account ID; do not erase history or invent deposits to make a bot run again. Keep test and
load databases separate: `simex_test` for tests, explicit fresh `simex_load_*` destinations for
the [load driver](load-test.md), never the trading `simex` database.

Bots read their **own process environment**; the exchange reading `.env` does not export it
to them. On the laptop supply the forwarded API port and token to the bot's environment, as in
the [quickstart](quickstart.md). Do not copy the server's ledger credentials to a remote bot.

## Start services and keep them supervised

From `/srv/simex`:

```bash
docker compose --profile monitoring config --quiet
docker compose up -d --wait
docker compose --profile monitoring up -d grafana
```

Compose currently starts **infrastructure only**. Run the ingestor, exchange and recorder as
separate host processes, each from the checkout with its virtual environment. For an initial
foreground check use separate terminals:

```bash
.venv/bin/python -m simex.ingest
# Another terminal:
.venv/bin/python -m simex.exchange
# Another terminal; needed for later offline evaluation:
.venv/bin/python -m simex.recorder
```

The exchange builds its own books and features. The separate `simex.book` process is optional
unless a consumer needs the Valkey book projection/audits it exports. Neither Grafana nor
Prometheus starts any of these Python processes.

For unattended operation, stop the foreground processes and install a unit such as the following
as `/etc/systemd/system/simex@.service`, substituting your owner and paths. It uses standard
[systemd service supervision](https://www.freedesktop.org/software/systemd/man/latest/systemd.service.html).
Keep `.env` to simple `NAME=value` assignments compatible with `EnvironmentFile`, not shell code.

```ini
[Unit]
Description=SimEx %i
Wants=network-online.target
After=network-online.target docker.service
Requires=docker.service

[Service]
Type=simple
User=simex
WorkingDirectory=/srv/simex
EnvironmentFile=/srv/simex/.env
ExecStart=/srv/simex/.venv/bin/python -m simex.%i
Restart=on-failure
RestartSec=5
TimeoutStopSec=60
UMask=0077

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now simex@ingest simex@exchange simex@recorder
sudo systemctl status simex@exchange
journalctl -u simex@exchange -n 50 --no-pager
```

Start infrastructure before these units; their dependency on Docker does not run Compose for
you. Containers have `restart: unless-stopped`; enable Docker's boot service on your host.
Do not start a second exchange process over the same account: there is one account writer.
Stop bots before maintenance and allow graceful shutdown; accepted orders may fill until the
exchange stops. Restarting discards queue/depletion state and restarts lag warm-up, as recorded
in [ADR 0016](adr/0016-stale-data-guard-and-fill-timing.md).

Check listener scope with `ss -lnt` and review cloud/host firewall rules. **The host Python
metrics servers bind all interfaces by default** on 9101–9104, unlike Compose/API loopback
ports. Block public access to them; permit the local Docker bridge to scrape them through
`host.docker.internal`, which the [Prometheus config](../infra/prometheus/prometheus.yml) uses.
Do not rely on a generic host firewall alone to protect published Docker ports; see Docker's
[firewall limitations](https://docs.docker.com/engine/install/ubuntu/#firewall-limitations).
Verify denial from outside the host before considering the deployment private.

Check `/v1/health` through your SSH forward, then an authenticated SDK quote/portfolio read
using the [quickstart](quickstart.md). Do not treat health as book readiness. Inspect the
provisioned [system dashboards](../infra/grafana/README.md): scrape `up`, gate state, catch-up,
watermark lag, reconciliation and halts. Optional processes intentionally left off produce
unavailable panels, not healthy zeroes. Absolute venue lag requires verified host clock
synchronization; check `timedatectl status` on the host, not only inside a container
([#74](https://github.com/Suddha-Abhitej/SimEx/issues/74)).
The ingestor logs one warning per sustained negative-lag episode and exports
`simex_ingest_clock_skew_suspected{venue,product_id}`. A value of 1 means a fresh condition
confirmed over 60 seconds with at least three observations; **0 is not evidence of clock
synchronization**, including during warm-up or a feed outage. It detects rather than repairs
clock disagreement; see [ADR 0025](adr/0025-ingestor-clock-skew-diagnostic.md) for episode and
freshness semantics. Earlier unavailable absolute-lag measurements remain unavailable.
After deploying #115, watch closures and the baseline-established gauge; see the
[ADR 0016 amendment](adr/0016-stale-data-guard-and-fill-timing.md#amendment-excess-lag-baseline-coverage-2026-11-09-113).

## Cloudflare Tunnel plus Access: the alternative

For phone/browser access, use an owner-controlled domain and Cloudflare account. Install the
ARM64 `cloudflared` connector on the server using the
[official local-tunnel guide](https://developers.cloudflare.com/cloudflare-one/networks/connectors/cloudflare-tunnel/do-more-with-tunnels/local-management/create-local-tunnel/).
Run it on the host so `127.0.0.1` reaches the existing services; it is not a Compose service.
Allow its required outbound connectivity, without opening inbound application ports.

**Before routing DNS or starting the connector**, create an Access self-hosted application for
each complete hostname below, covering all paths. Allow only the owner's exact identity through
your chosen login provider, and require MFA there. No Everyone/Bypass policy or public dashboard.
Access is deny-by-default once an application exists; a tunnel alone supplies no owner check.
Follow [Cloudflare's Access setup](https://developers.cloudflare.com/cloudflare-one/access-controls/applications/http-apps/self-hosted-public-app/).
Keep Grafana's login enabled and the exchange bearer token required behind Access as well.

Following the guide, create a named tunnel locally:

```bash
cloudflared tunnel login
cloudflared tunnel create simex-private
```

Save this as the connector user's private `~/.cloudflared/config.yml`, replacing the tunnel
ID, credential path and hostnames. Both hostnames must already be protected by Access.
Omit the API route if only phone/browser dashboard access is needed.

```yaml
tunnel: <tunnel-uuid>
credentials-file: /home/simex/.cloudflared/<tunnel-uuid>.json
ingress:
  - hostname: grafana.example.com
    service: http://127.0.0.1:3000
  - hostname: bot.example.com
    service: tcp://127.0.0.1:8770
  - service: http_status:404
```

The API route is TCP so a client-side authenticated bridge can carry both HTTP and the fills
WebSocket without SDK changes. Validate routing before running:

```bash
cloudflared tunnel ingress validate
cloudflared tunnel ingress rule https://grafana.example.com
cloudflared tunnel ingress rule https://bot.example.com
cloudflared tunnel route dns simex-private grafana.example.com
cloudflared tunnel route dns simex-private bot.example.com
cloudflared tunnel run simex-private
```

The final 404 prevents unmatched hosts from reaching an origin
([configuration reference](https://developers.cloudflare.com/tunnel/features/locally-managed-tunnels/configuration-file/)).
For ongoing use install the connector as a supervised service using Cloudflare's guide, with
the explicit config/credential paths; never commit its JSON, certificate or token. Test from
a signed-out browser that Access blocks Grafana, then sign in as the owner and confirm Grafana
still asks for its own credentials. A phone needs only the browser for this path.

On another computer with `cloudflared`, authenticate the API bridge interactively:

```bash
cloudflared access tcp --hostname bot.example.com --url 127.0.0.1:8770
```

This launches owner authentication and provides a local socket, as in Cloudflare's
[client authentication](https://developers.cloudflare.com/cloudflare-one/access-controls/applications/non-http/cloudflared-authentication/)
and [TCP forwarding example](https://developers.cloudflare.com/cloudflare-one/tutorials/kubectl/).
The bot connects to that loopback port with its normal API bearer token. Test that a signed-out
bridge cannot access the API; do not bypass Access to make an unattended client work. A direct
SDK call to the hostname cannot complete an interactive Access login.
Cloudflare notes that [long-lived TCP connections need a different client-to-tunnel path](https://developers.cloudflare.com/tunnel/concepts/routing/);
use SSH first for a long-running fills stream. A tunnel disconnect still requires the SDK's
explicit checkpoint/gap handling; it is not permission to restart from scratch silently.

## Resource budget and pressure

These are **configured container memory ceilings**, not reservations, observed usage or tested
Oracle capacity. All services compete for the same two cores; Compose sets no CPU ceiling here.

| Service | Compose memory cap | Role |
| --- | ---: | --- |
| Kafka | 1280 MiB | Live market-data transport; JVM heap max 768 MiB |
| Postgres | 1 GiB | Authoritative ledger, 50 configured maximum connections |
| Valkey | 256 MiB | Derived cache/book projection; 192 MiB data max with LRU eviction |
| Prometheus | 512 MiB | Scrapes and the existing alert rules; starts by default |
| Grafana | 256 MiB | Optional `monitoring` profile; dashboards only |

The default infrastructure caps total about 3 GiB, plus 256 MiB for Grafana. Host Python
processes, `cloudflared`, OS/page cache and other software are outside these caps. Budget for
them; unused capacity below one container's limit is not an allocation to another service.
The lean ingestor/exchange/recorder stack is the intended workload, not an ARM64 throughput
claim. Keep model inference, bulk replay, load tests and heavy analytics on another machine
such as the home archive host; no shared two-core headroom has been demonstrated for them.

Under pressure stop optional UI first, preserving its volume:

```bash
docker compose --profile monitoring stop grafana
# If necessary: loses scrapes, history during downtime, and Prometheus rule evaluation.
docker compose stop prometheus
```

Only Grafana is profile-gated today; leaving off `--profile monitoring` does not stop an
already-running Grafana, and does not disable Prometheus. Restart deliberately when pressure
clears. Inspect `docker stats --no-stream`, host memory, disk and service logs. Pause bots and
heavy analysis before compromising Kafka or Postgres; do not raise all caps blindly, shorten
the delay, disable gates or bypass reconciliation to recover throughput. Stop the optional
book process only when nobody needs its projection. Stopping the recorder sacrifices future
evaluation coverage; keep track of the gap rather than calling that a harmless optimization.

## Storage, backup and recovery

Mount the data disk before starting services. Check Docker's actual volume location with
`docker info --format '{{.DockerRootDir}}'`; named volumes are not automatically moved to the
data disk by this repository. Place Docker storage and private recordings appropriately before
first startup; do not move live volume files underneath running containers. Keep room for WAL,
open Parquet files, Kafka, container logs and the ledger in addition to completed recordings.

Recorder retention defaults to 30 days under `RECORDER_ROOT`. With `RECORDER_REQUIRE_ARCHIVED=1`,
expired days wait for a verified archive's `_ARCHIVED` marker. **The archive job is not supplied**:
the owner must schedule private off-host copies, verify them and mark only verified days.
Without a working archive, this protection lets disk usage grow. Published files are hourly;
clean shutdown also publishes partial hours. Do not copy active `.tmp` files as completed data.
See [ADR 0013](adr/0013-recorder-parquet-layout-exactly-once-retention.md).

Kafka level2 defaults to 24-hour retention and 2 GiB per partition; these do not bound every
topic or the whole data disk. Prometheus retention is configured to 15 days/2 GB; its live
storage has overhead too. Single-node Kafka and Postgres volumes are not replicated backups.
Never run `docker compose down -v` as a routine restart: it deletes persistent volumes.

Back up the ledger independently of recordings. For example, with a private backup directory
outside the checkout, using the container's own configured database/user:

```bash
umask 077
backup_dir=/your/private/backup/path
mkdir -p "$backup_dir"
backup_file="$backup_dir/ledger-$(date -u +%Y%m%dT%H%M%SZ).dump"
docker compose exec -T postgres sh -c \
  'pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Fc' > "$backup_file.partial" &&
  mv "$backup_file.partial" "$backup_file"
```

Only successful dumps become the final file; inspect failures and keep a verified off-host
copy. [Postgres SQL dumps](https://www.postgresql.org/docs/16/backup-dump.html) give a consistent
database snapshot, not a common snapshot with Kafka/recordings. Back up roles/credentials and
private configuration separately. Rehearse `pg_restore --exit-on-error` into a **fresh isolated
database**, verifying orders, postings, fills and recovery/reconciliation before relying on a
backup. Never restore a test over the trading ledger, delete audit history, or repair balances
with ad hoc SQL. Dumps, recordings, reports and credentials remain private.

For updates, stop bots, back up first, gracefully stop host services, update the checkout and
environment dependencies, then restart one exchange writer against the same ledger. Startup
applies migrations and restores committed state. Inspect reconciliation and gate readiness
before bots resume. A stopped API, a halt or a stream gap is not a cancellation acknowledgement;
reconcile outstanding orders and preserve submission keys. See the
[private API contract](private-api.md) and [ledger failure/recovery semantics](adr/0017-event-sourced-ledger.md).

## What can be shared

[ADR 0010](adr/0010-personal-use-and-gemini-market-data.md) governs this deployment and any
write-up: architecture and system aggregates such as throughput, lag, gaps, latency and
reconciliation can be shared. A Grafana screenshot of system panels is shareable; prices,
recordings, agent results and equity curves are not. No public market-data/results page,
download link, anonymous dashboard or unauthenticated API. Cloudflare/SSH access remains for
the owner only; a private tunnel does not grant redistribution rights.
