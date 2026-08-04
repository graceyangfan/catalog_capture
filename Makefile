CARGO ?= cargo
# Keep in sync with rust-toolchain.toml
TOOLCHAIN ?= 1.97.1
CARGO_TOOL := $(CARGO) +$(TOOLCHAIN)
CARGO_DENY_VERSION ?= 0.19.9

# Product binary only (single entrypoint).
CLI_PKG := catalog-capture-cli

# Cloud multi-venue capture (no bybit/okx in the link graph).
CAPTURE_FEATURES ?= venue-binance,venue-deribit,venue-hyperliquid

.PHONY: bootstrap-deps build build-slim build-release build-release-capture build-release-small \
	test test-lib fmt clippy pre-commit cargo-deny install-tools \
	smoke-soak cleanup-tmp run-service clean clean-debug clean-all-targets help

help:
	@echo "Product binary: $(CLI_PKG) only."
	@echo ""
	@echo "Build (smaller / faster):"
	@echo "  build-release-capture  release + only venues needed for multi-venue mainnet"
	@echo "                         (FEATURES=$(CAPTURE_FEATURES))"
	@echo "  build-release          release, all venues (largest graph)"
	@echo "  build-release-small    --profile release-small (slower, smaller binary)"
	@echo "  build-slim             debug slim: FEATURES=venue-deribit (override FEATURES=...)"
	@echo "  clean / clean-debug    wipe this repo target/ (not ../nautilus_trader/target)"
	@echo "  clean-all-targets      also wipe sibling nautilus_trader/target (frees tens of GB)"
	@echo ""
	@echo "Other: bootstrap-deps, test, test-lib, clippy, run-service CONFIG=..."

bootstrap-deps:
	./scripts/bootstrap-deps.sh

build:
	$(CARGO_TOOL) build -p $(CLI_PKG)

# Slim product CLI. Override: make build-slim FEATURES=venue-binance,venue-hyperliquid
FEATURES ?= venue-deribit
build-slim:
	$(CARGO_TOOL) build -p $(CLI_PKG) --no-default-features --features $(FEATURES)

build-release:
	$(CARGO_TOOL) build --release -p $(CLI_PKG)

# Recommended for cloud multi-venue capture: fewer adapters → less disk + faster link.
build-release-capture:
	$(CARGO_TOOL) build --release -p $(CLI_PKG) --no-default-features --features $(CAPTURE_FEATURES)

build-release-small:
	$(CARGO_TOOL) build --profile release-small -p $(CLI_PKG) --no-default-features --features $(CAPTURE_FEATURES)

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
	rm -rf target/debug target/tmp

# Frees the usual multi-10GB piles: this target/ + sibling NT target used when
# developing nautilus_trader itself (nextest/incremental/doc).
clean-all-targets:
	$(CARGO_TOOL) clean
	@if [ -d ../nautilus_trader/target ]; then \
		echo "Removing ../nautilus_trader/target ..."; \
		rm -rf ../nautilus_trader/target; \
	fi

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
