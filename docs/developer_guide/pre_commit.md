# Pre-commit

```bash
pip install pre-commit
pre-commit install
pre-commit run --all-files
# or
make pre-commit
```

Hooks cover formatting, clippy-related checks, and repo hygiene configured in
`.pre-commit-config.yaml`. Fix failures locally before pushing.
