# Optional service helpers

**Default workflow:** stay in the repo, build, and run. No system install.

```bash
make build-release
./scripts/run-capture-service.sh \
  --config examples/operator/capture.deribit-btc-universe-unattended.toml \
  --release
```

Catalogs write under `./data/…` (see example TOML). Logs under `./logs/`.

If you want a **user-level** launchd/systemd unit that points at **this clone**,
run (optional, self-service):

```bash
./scripts/optional-user-service.sh --platform launchd \
  --config examples/operator/capture.deribit-btc-universe-unattended.toml

./scripts/optional-user-service.sh --platform systemd \
  --config examples/operator/capture.deribit-btc-universe-unattended.toml
```

The script only prints paths / writes unit files under your home directory
(`~/Library/LaunchAgents` or `~/.config/systemd/user`). It does **not** install
into `/opt`, `/var/lib`, or system-wide unit directories unless you change it yourself.

The generated systemd unit sends recorder output to the journal. A capture
deadline, shutdown signal, or pressure limit requests a graceful stop and asks
systemd to stop the user unit. Systemd enforces the unit's `TimeoutStopSec=60`
even if a writer is stuck. The recorder treats 30 seconds of continuous writer
work, waiting work, or a stale actor heartbeat as a stall. The writer can be
stalled even with an empty queue. Shutdown signals all writer families before
waiting for any of them, allowing healthy families to seal their files.
Pressure-triggered unit stops require a manual restart. Unsupervised CLI runs use the recorder's graceful stop path.

Before a long run on Linux, build the CLI tests with
`./scripts/cargo.sh test -p catalog-capture-cli --no-run`. Use the reported test executable to exercise
the stalled-writer probe under a disposable user unit:

```bash
unit="capture-stop-probe-$$.service"
systemd-run --user --wait --unit="$unit" --property=TimeoutStopSec=2s \
  --setenv=CAPTURE_TEST_SYSTEMD_UNIT="$unit" \
  "$test_binary" --exact pressure::tests::stalled_writer_process --ignored
systemctl --user show "$unit" -p Result -p ExecMainCode -p ExecMainStatus
```

The probe should report a stop timeout and SIGKILL. It starts a deliberately
stalled test sink without connecting to a venue or writing a catalog.
