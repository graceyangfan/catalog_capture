CARGO ?= cargo
TOOLCHAIN ?= 1.96.0
CARGO_TOOL := $(CARGO) +$(TOOLCHAIN)

.PHONY: build build-release test fmt clippy smoke-soak cleanup-tmp run-service help

help:
	@echo "Targets:"
	@echo "  build          Build debug catalog-capture-cli"
	@echo "  build-release  Build release catalog-capture-cli"
	@echo "  test           Run workspace unit tests"
	@echo "  fmt            Run rustfmt"
	@echo "  clippy         Run clippy on workspace"
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

smoke-soak:
	python3 tests/probe_option_universe_soak.py --preset daily-live --seconds 180 --cleanup

cleanup-tmp:
	./scripts/cleanup-tmp-captures.sh

run-service:
	@test -n "$(CONFIG)" || (echo "CONFIG is required, e.g. make run-service CONFIG=examples/operator/capture.deribit-btc-universe-unattended.toml" && exit 2)
	./scripts/run-capture-service.sh --config "$(CONFIG)" --release