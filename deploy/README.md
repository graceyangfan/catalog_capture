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
