# Predict evidence

Primary captures have priority over replay and synthetic outputs. The crate-level
`../evidence_manifest.json` records exact bytes, SHA-256 and evidence tiers; it
includes completed raw node/collector logs as well as JSON/JSONL. This is an index,
not permission to overwrite a capture or upgrade an inferred shape to venue fact.

- **primary_capture**: sanitized actual REST/wallet/chain/Native observations or
  public capture fixtures. Successful and failed real runs are both retained.
- **derived**: replay, projection or analysis based on captured economics. Market,
  token, identity or timing rebinding does not establish new venue behavior.
- **synthetic**: deterministic local fault/ordering/precision cases. Keep cases with
  executable coverage; unused superseded snapshot exports may be removed.
- **review/reference**: validation summaries, SDK/schema vectors and sources whose
  provenance is not sufficient to call a primary venue capture.

The tags are conservative. The source's own metadata and retained response shape
remain authoritative; a filename containing "actual" is not sufficient proof.

Important complete runs:

- `live_resting_fixed_20261015/` and maker/batch captures: accepts, cancellations,
  standard rejections and no-fill controls, plus original failure diagnostics.
- `live_fill_20261016T123240Z/` and its independent directory: original funded
  order economics used by Native replay.
- `live_fill_20261017T051620Z/`: actual BUY32, SHARES fee .64, net31.36, TP and
  preserved interruption state.
- `exit_only_restart_20261017T052850Z/`: nonempty empty-Cache startup/adoption,
  no extra BUY or duplicate take-profit.
- `exit_only_restart_20261017T053156Z/` and its independent directory: actual
  TP cancellation, MARKET SELL, callbacks, matches/chain and flat final account.
- `session_readonly_20261017T045625Z_69675.json`: read-only JWT renewal,
  resubscription, QueryAccount and readiness; earlier failure captures retained.

Normalized Native journals are not unmodified venue envelopes. Preserve each
run's raw wallet JSONL, HTTP responses, independent chain/matches, Native callback
journal and original log together. Identities/signatures are sanitized; never
store credentials or JWTs. Public snapshot captures and schema/SDK vectors retain
their own provenance. Missing venue snapshots do not prove an empty account.

```bash
python3 tests/predict_evidence.py                 # verify immutable corpus
python3 tests/predict_evidence.py --refresh       # add completed NEW captures
```

Stop collectors before indexing. Refresh rejects changed existing primary bytes.
The raw collectors and guarded supervisors remain under root `tests/`; follow the
[operator guide](../../../../../docs/how_to/predict-rust-strategy-calibration.md).
Ordinary tests do not authenticate or trade. Do not run ignored mainnet entry
points as part of an offline cleanup.
