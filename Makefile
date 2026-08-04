CARGO ?= cargo
# Keep in sync with rust-toolchain.toml
TOOLCHAIN ?= 1.97.1
CARGO_TOOL := $(CARGO) +$(TOOLCHAIN)
CARGO_DENY_VERSION ?= 0.19.9

# Product binary only (single entrypoint).
CLI_PKG := catalog-capture-cli

.PHONY: bootstrap-deps build build-slim build-release test test-lib fmt clippy pre-commit cargo-deny install-tools \
	smoke-soak cleanup-tmp run-service clean clean-debug help

help:
	@echo "Product binary: $(CLI_PKG) only (no cargo examples in the product path)."
	@echo ""
	@echo "Targets:"
	@echo "  bootstrap-deps Prepare sibling nautilus_trader (prefer local; else clone develop)"
	@echo "  build          Build debug $(CLI_PKG) (default features = all-venues)"
	@echo "  build-slim     Slim CLI: --no-default-features --features \$$FEATURES (default venue-deribit)"
	@echo "  build-release  Build release $(CLI_PKG)"
	@echo "  test           Run workspace unit tests (libs + cli unit tests)"
	@echo "  test-lib       Run core + runtime-adapter lib tests only"
	@echo "  fmt            Run rustfmt"
	@echo "  clippy         Run clippy on workspace libraries/cli (no examples)"
	@echo "  clean          cargo clean (full target/)"
	@echo "  clean-debug    Remove target/debug only"
	@echo "  pre-commit     Run all pre-commit hooks"
	@echo "  cargo-deny     Run cargo-deny license checks"
	@echo "  install-tools  Install pinned cargo-deny"
	@echo "  smoke-soak     Run daily-live soak (180s, with cleanup)"
	@echo "  cleanup-tmp    Remove ./data capture artifacts (or pass a dir)"
	@echo "  run-service    Run unattended capture (CONFIG=... required)"

bootstrap-deps:
	./scripts/bootstrap-deps.sh

build:
	$(CARGO_TOOL) build -p $(CLI_PKG)

# Slim product CLI (example: Deribit-only). Override: make build-slim FEATURES=venue-binance
FEATURES ?= venue-deribit
build-slim:
	$(CARGO_TOOL) build -p $(CLI_PKG) --no-default-features --features $(FEATURES)

build-release:
	$(CARGO_TOOL) build --release -p $(CLI_PKG)

test:
	$(CARGO_TOOL) test --workspace --lib --bins

test-lib:
	$(CARGO_TOOL) test -p catalog-capture-core --lib
	$(CARGO_TOOL) test -p catalog-capture-runtime-adapter --lib

fmt:
	$(CARGO_TOOL) fmt --all

clippy:
	$(CARGO_TOOL) clippy -p catalog-capture-core -p catalog-capture-runtime-adapter -p $(CLI_PKG) --all-targets -- -D warnings

clean:
	$(CARGO_TOOL) clean

clean-debug:
	rm -rf target/debug

install-tools:
	@if ! cargo deny --version 2>/dev/null | grep -q "$(CARGO_DENY_VERSION)"; then \
		cargo install cargo-deny --version $(CARGO_DENY_VERSION) --locked; \
	fi

cargo-deny: install-tools
	cargo deny check licenses

pre-commit:
	pre-commit run --all-files

smoke-soak:
	python3 tests/probe_option_universe_soak.py --preset daily-live --seconds 180 --cleanup

cleanup-tmp:
	./scripts/cleanup-tmp-captures.sh

run-service:
	@test -n "$(CONFIG)" || (echo "CONFIG is required, e.g. make run-service CONFIG=examples/operator/capture.deribit-btc-universe-unattended.toml" && exit 2)
	./scripts/run-capture-service.sh --config "$(CONFIG)" --release
