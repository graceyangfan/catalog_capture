CARGO := $(CURDIR)/scripts/cargo.sh
# Keep in sync with rust-toolchain.toml
TOOLCHAIN ?= 1.99.1
CARGO_TOOL := $(CARGO) +$(TOOLCHAIN)
CARGO_DENY_VERSION ?= 0.19.9

# Product binary only (single entrypoint).
CLI_PKG := catalog-capture-cli
BIN_DIR ?= bin
PRODUCT_BIN := $(BIN_DIR)/catalog-capture-cli

# Cloud multi-venue capture (only adapters needed by the capture service).
CAPTURE_FEATURES ?= venue-binance,venue-deribit,venue-hyperliquid,venue-lighter

.PHONY: bootstrap-deps bootstrap-deps-local build build-slim build-release \
	build-release-capture build-release-small \
	package-cloud \
	test test-lib fmt clippy pre-commit cargo-deny install-tools \
	smoke-soak cleanup-tmp run-service clean help

.PHONY: install-cargo cache-size

help:
	@echo "Product binary: $(CLI_PKG) only."
	@echo "  product binary: $(PRODUCT_BIN) (target/ is disposable build cache)"
	@echo ""
	@echo "Build (smaller / faster):"
	@echo "  build-release-capture  release + venues used by the standard mainnet profile"
	@echo "                         (FEATURES=$(CAPTURE_FEATURES))"
	@echo "  build-release          release, all venues (largest graph)"
	@echo "  build-release-small    --profile release-small (slower, smaller binary)"
	@echo "  package-cloud          build a self-contained cloud deployment tarball"
	@echo "  build-slim             debug slim: FEATURES=venue-deribit (override FEATURES=...)"
	@echo "  clean                  clear this project's local and central build cache"
	@echo "  cache-size             inspect target/ and the central Cargo cache"
	@echo ""
	@echo "Other: bootstrap-deps (--pin-ci), bootstrap-deps-local, test, clippy, run-service..."
	@echo "Tool setup: make install-cargo (pinned huacnlee/cargo; shared cache with automatic GC)"

install-cargo:
	./scripts/cargo.sh --install

cache-size:
	./scripts/cargo.sh --cache-size

# First-time / reproducible: match CI Nautilus pin.
bootstrap-deps: install-cargo
	./scripts/bootstrap-deps.sh --pin-ci

# Existing editable sibling tree — do not force-checkout the CI pin.
bootstrap-deps-local: install-cargo
	./scripts/bootstrap-deps.sh

build:
	$(CARGO_TOOL) build -p $(CLI_PKG)

# Slim product CLI. Override: make build-slim FEATURES=venue-binance,venue-hyperliquid
FEATURES ?= venue-deribit
build-slim:
	$(CARGO_TOOL) build -p $(CLI_PKG) --no-default-features --features $(FEATURES)

build-release:
	$(CARGO_TOOL) build --release -p $(CLI_PKG)
	@mkdir -p $(BIN_DIR)
	cp target/release/$(CLI_PKG) $(PRODUCT_BIN)

# Recommended for cloud multi-venue capture: fewer adapters → less disk + faster link.
build-release-capture:
	$(CARGO_TOOL) build --release -p $(CLI_PKG) --no-default-features --features $(CAPTURE_FEATURES)
	@mkdir -p $(BIN_DIR)
	cp target/release/$(CLI_PKG) $(PRODUCT_BIN)

build-release-small:
	$(CARGO_TOOL) build --profile release-small -p $(CLI_PKG) --no-default-features --features $(CAPTURE_FEATURES)
	@mkdir -p $(BIN_DIR)
	cp target/release-small/$(CLI_PKG) $(PRODUCT_BIN)

package-cloud:
	./scripts/package-cloud.sh

test:
	$(CARGO_TOOL) test --workspace --lib --bins

test-lib:
	$(CARGO_TOOL) test -p catalog-capture-core --lib
	$(CARGO_TOOL) test -p catalog-capture-runtime-adapter --lib

fmt:
	$(CARGO_TOOL) fmt --all

clippy:
	$(CARGO_TOOL) clippy -p nautilus-lighter-ext -p nautilus-predict -p catalog-capture-core -p catalog-capture-runtime-adapter -p $(CLI_PKG) --all-targets -- -D warnings

clean:
	$(CARGO_TOOL) clean

install-tools: install-cargo
	@if ! $(CARGO_TOOL) deny --version 2>/dev/null | grep -q "$(CARGO_DENY_VERSION)"; then \
		$(CARGO_TOOL) install cargo-deny --version $(CARGO_DENY_VERSION) --locked; \
	fi

cargo-deny: install-tools
	$(CARGO_TOOL) deny check licenses

pre-commit:
	pre-commit run --all-files

smoke-soak:
	python3 tests/probe_option_universe_soak.py --preset daily-live --seconds 180 --cleanup

cleanup-tmp:
	./scripts/cleanup-tmp-captures.sh

run-service:
	@test -n "$(CONFIG)" || (echo "CONFIG is required, e.g. make run-service CONFIG=examples/operator/capture.deribit-btc-universe-unattended.toml" && exit 2)
	./scripts/run-capture-service.sh --config "$(CONFIG)" --release
