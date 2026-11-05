# Build cache and disk usage

## Standard build tool

The project uses [huacnlee/cargo](https://github.com/huacnlee/cargo) through
`scripts/cargo.sh`. Its version and SHA-256 checksums are pinned in that script.
Make targets, service/packaging scripts, pre-commit hooks, and CI all use this
entry point. The wrapper operates from the workspace root and puts its pinned
binary first on child processes' PATH, including external Cargo subcommands.
Automatic updates are disabled; upgrades require changing the pin and checksums.

```bash
make install-cargo
make build
make test
# Direct Cargo commands:
./scripts/cargo.sh check -p catalog-capture-core --lib
```

Rust 1.99.1, rustfmt, and Clippy are supplied by rustup as specified in
`rust-toolchain.toml`. Installing project Cargo does not change another
repository's toolchain.

## Where disk space goes

| Location | Contents |
|----------|----------|
| `.tools/cargo/<version>/cargo` | Pinned build tool |
| `target/` | Final binaries and test output |
| `~/.cargo/targets/<project>-<hash>/` | Project intermediates and incremental caches, shared by its worktrees |
| `~/.cargo/targets/shared-deps/` | Registry/git dependency builds shared across projects |
| `bin/catalog-capture-cli` | Installed product binary used for deployment |

`CARGO_HOME`, when set, replaces `~/.cargo` in the cache paths. Local Nautilus
path dependencies still bring a large crate graph into the project build.
A small local `target/` alone does not indicate total build disk usage.
The fork ignores `--target-dir`, `CARGO_TARGET_DIR`, and build-directory
configuration; these cannot isolate or relocate its caches.

## Automatic collection

The single cache policy lives in `.cargo/config.toml`:

- Project caches use an adaptive limit: twice the largest build in the last day.
- Project units unused for one day are eligible for eviction.
- Shared dependencies have a 20 GiB limit and seven-day retention.
- Collection runs after builds at most once an hour.

These are eviction policies, not a hard cap on peak disk usage during a build.
The existing dev/test profiles disable debug info and retain incremental
compilation; release profiles disable incremental compilation. Automatic GC
bounds accumulation while keeping useful rebuild caches.

## Inspect and reclaim space

```bash
make cache-size   # local target plus all central Cargo caches
make clean        # this project's local and central build cache
```

Run cleanup after this project's builds/tests have stopped. Shared dependencies
remain available to other projects after `make clean`. Deleting only `target/`
does not reclaim the central intermediates. Keep deployed binaries in `bin/`,
which is independent of disposable build caches.

If the global shared dependency cache must also be removed, first stop all
builds using it, then remove only that directory:

```bash
rm -rf "${CARGO_HOME:-$HOME/.cargo}/targets/shared-deps"
```

The next build will recompile shared dependencies. This project's cleanup does
not wipe the sibling Nautilus build tree.

For a complete reset of the fork's build caches, stop builds/tests in **all**
repositories using the fork, then remove the entire central cache and this
project's local output:

```bash
make clean
rm -rf "${CARGO_HOME:-$HOME/.cargo}/targets"
```

This also removes other projects' central build caches. They are disposable and
will be rebuilt; use the narrower cleanup above when those caches should stay.
`target/` is disposable too: removing it deletes local binaries and test output.
Temporary build/test artifacts in these directories need not be preserved after
validation. Keep the pinned tool in `.tools/cargo/` so the next build can run.

The central directory is **`~/.cargo/targets/`** (plural). Do not remove the whole
`~/.cargo/` directory to clean build output: it also contains installed tools,
configuration, and possibly registry credentials. Its `registry/` and `git/`
directories cache dependency sources, rather than build output; deleting those
requires downloading dependencies again and affects offline builds.

## Smaller product builds

Compile only the venues needed by the capture service:

```bash
# Standard Binance + Deribit + Hyperliquid + Lighter graph:
make build-release-capture
# Single-venue development:
make build-slim FEATURES=venue-deribit
# Smaller final binary, with slower linking:
make build-release-small
```

These release targets install `bin/catalog-capture-cli`. Run capture services
from that installed binary so build-cache collection does not affect the
deployed executable. Avoid full Nautilus workspace tests, nextest archives, or
API documentation builds on capture-only servers unless they are required.
