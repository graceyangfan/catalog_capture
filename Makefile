CARGO ?= cargo
TOOLCHAIN ?= 1.96.0
CARGO_TOOL := $(CARGO) +$(TOOLCHAIN)
CARGO_DENY_VERSION ?= 0.19.9

.PHONY: build build-release test fmt clippy pre-commit cargo-deny install-tools smoke-soak cleanup-tmp run-service help

help:
	@echo "Targets:"
	@echo "  build          Build debug catalog-capture-cli"
	@echo "  build-release  Build release catalog-capture-cli"
	@echo "  test           Run workspace unit tests"
	@echo "  fmt            Run rustfmt"
	@echo "  clippy         Run clippy on workspace"
	@echo "  pre-commit     Run all pre-commit hooks"
	@echo "  cargo-deny     Run cargo-deny license checks"
	@echo "  install-tools  Install pinned cargo-deny"
	@echo "  smoke-soak     Run daily-live soak (180s, with cleanup)"
	@echo "  cleanup-tmp    Remove /tmp smoke/soak capture artifacts"
	@echo "  run-service    Run unattended capture (CONFIG=... required)"

build:
	$(CARGO_TOOL) build -p catalog-capture-cli

build-release:
	$(CARGO_TOOL) build --release -p catalog-capture-cli

test:
	$(CARGO_TOOL) test --workspace

fmt:
	$(CARGO_TOOL) fmt --all

clippy:
	$(CARGO_TOOL) clippy --workspace --all-targets -- -D warnings

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