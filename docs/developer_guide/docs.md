# Documentation style

Follow [Nautilus Trader docs conventions](https://github.com/nautechsystems/nautilus_trader/blob/develop/docs/developer_guide/docs.md)
where they apply:

- Use sentence case for headings (H2 and below).
- Prefer active voice and present tense.
- Separate tutorial, how-to, concept, and reference content (Divio system).

## This repository

| Section | Use for |
|---------|---------|
| `getting_started/` | First install and quickstart |
| `developer_guide/` | Contributor setup and tooling |
| `how_to/` | Operator tasks (soak, unattended, cleanup) |
| `reference/` | CLI subcommands and TOML fields |
| Top-level `docs/*.md` | Architecture and design (concepts) |

When adding operator-facing behavior, update both the relevant how-to page and
`examples/README.md` if a new example config is introduced.