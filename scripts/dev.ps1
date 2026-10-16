# Local stack shortcuts for Windows (same targets as the Makefile).
# Usage: .\scripts\dev.ps1 up | down | logs [service] | ps | smoke | ingest | book | record
param(
    [Parameter(Position = 0)][ValidateSet("up", "down", "logs", "ps", "smoke", "ingest", "book", "record")][string]$Command = "ps",
    [Parameter(Position = 1)][string]$Service
)
$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $PSScriptRoot)

switch ($Command) {
    "up"    { docker compose up -d --wait }
    "down"  { docker compose down }
    "logs"  { if ($Service) { docker compose logs -f --tail=100 $Service } else { docker compose logs -f --tail=100 } }
    "ps"    { docker compose ps }
    "smoke" { python scripts/smoke_test.py }
    "ingest" { python -m simex.ingest }
    "book"   { python -m simex.book }
    "record" { python -m simex.recorder }
}
exit $LASTEXITCODE
