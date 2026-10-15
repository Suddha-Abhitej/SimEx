# 0006. $0 hosting: Oracle Always Free for production, home server for archive

- **Status:** Accepted
- **Date:** 2026-10-14

## Context

The first priority is to run SimEx for free, the second to run it efficiently. Paid cloud hosting
(AWS EC2 + S3 in the first plan) would cost tens of dollars a month for a 24/7 stack. Oracle Cloud's
Always Free tier includes one Ampere A1 ARM64 server with 2 cores and 12 GB of RAM, plus 200 GB of
block storage, which fits the stack with memory caps. A home server (Ryzen 7, 16 GB RAM, 2 TB disk)
is available but depends on home power and internet.

## Decision

- **Production** runs on Oracle Always Free (VM.Standard.A1.Flex, 2 OCPU, 12 GB, Ubuntu 24.04,
  50 GB boot + 150 GB data disk) as one Docker Compose stack: Kafka (KRaft), Postgres, Valkey,
  SimEx services, and a metrics agent.
- **Public access** goes through Cloudflare Tunnel. Only SSH is open in Oracle's security list; all
  service ports bind to 127.0.0.1.
- **Recordings** keep about 30 days on Oracle and sync nightly to the **home server**, which holds
  the long-term archive and runs the Replay Playground, backtests and analytics.
- **Website** is hosted on a free static host (Azure Static Web Apps or Cloudflare Pages).
- Every image must support `linux/arm64`; CI builds multi-arch images.

## Consequences

- $0 hosting, with production in a data center and storage-heavy work on hardware already owned.
- Tight resources: 2 cores and 12 GB require memory limits on every container and a lean feature
  pipeline. Flink may be replaced by lighter Python jobs if CPU runs short.
- Free Ampere capacity is often unavailable at signup, and Oracle reduced this tier once in 2026.
  Because everything runs in Docker Compose, the stack can move to the home server in about an hour.
- The resume keeps infrastructure-as-code (Terraform) and adds ARM64 and multi-cloud experience,
  but loses hands-on AWS hosting; LocalStack covers S3-style testing.
