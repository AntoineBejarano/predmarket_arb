# RUNBOOK — exogenous_compact

Reproduce the compact exogenous evaluation after local data exists under `data/raw/` (gitignored).

## Preconditions

```bash
python3.12 setup.py && source .venv/bin/activate
python download_datasets.py   # {ASSET}_1min.parquet in data/raw/
```

## Pipeline (order)

```bash
# 1) Binance UM + optional Polymarket history → data/raw/exogenous/ (large)
python scripts/download_exogenous_compact.py

# 2) Join → data/raw/exogenous/compact_5m/{ASSET}.parquet
python scripts/build_exogenous_features.py

# 3) Logistic baseline vs baseline+exo (writes into this folder by default)
python scripts/evaluate_compact_vs_baseline.py

# 4) Saved PKL vs majority on last ~180d
python scripts/freeze_baseline_compact.py

# 5) Conservative GO/NO-GO from eval JSON
python scripts/compact_go_no_go.py
```

## Output directory override

All of steps 3–5 accept `--experiment-dir PATH` or env `PM_STRATEGY_EXPERIMENT_DIR` (repo-relative or absolute). Default is this folder.

## Reference snapshot

- Reports committed as of **2026-04-27** (see `LEARNINGS.md` and JSON files in this directory).
