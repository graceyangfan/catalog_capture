# Security policy

## Supported versions

| Version | Supported |
|---------|-----------|
| 0.1.x   | Yes (best effort) |
| main    | Yes (development) |

## Reporting a vulnerability

Please **do not** open a public GitHub issue for security problems that could
enable remote compromise, secret leakage, or silent data corruption.

Instead, email the maintainer contact on the GitHub profile for this repository,
or open a **private** security advisory on GitHub if available.

Include:

- affected version / commit
- description of the issue
- reproduction steps or proof-of-concept (non-destructive)
- impact assessment if known

You should receive an acknowledgement within a reasonable time. We will
coordinate disclosure after a fix is available when practical.

## Scope notes

- Capture often uses **public market data**; optional API keys must stay in
  environment variables (never committed configs).
- Report issues that could write attacker-controlled paths outside the configured
  catalog root, leak credentials into logs, or corrupt sealed Parquet segments.
