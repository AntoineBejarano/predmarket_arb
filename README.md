# PredMarket Arb — Polymarket strategy lab

Paper-trading and **edge-validation** against Polymarket-style markets (today: **5-minute crypto Up or Down**). The stack compares market-implied probabilities (Gamma) with an ML model (LightGBM + isotonic calibration) and a NearRes heuristic. **No trading or wallet**—public APIs and CSV logs only.

The repository is structured as a **strategy laboratory**: each hypothesis lives under `strategies/<strategy_id>/` with a manifest (`strategy.yaml`) and **immutable-style experiment folders** (`experiments/<slug>/`) that store **how** you ran work (`RUNBOOK.md`), **what you learned** (`LEARNINGS.md`), and small metrics/spec files (JSON/Markdown) suitable for git. Domains can grow beyond crypto (sports, politics, …) by adding new strategy trees and scripts while reusing shared clients where it makes sense.

- Lab layout and conventions: [strategies/README.md](strategies/README.md)  
- Ops / logs (Spanish): [CLAUDE.md](CLAUDE.md)  
- Default path helper for compact pipelines: `lab/paths.py` (env `PM_STRATEGY_EXPERIMENT_DIR` overrides output dir)

---

## What you get in this repo

| Piece | Role |
|--------|------|
| `scripts/api.py` | FastAPI: `/`, `/api/status`, worker START/STOP, SSE `/api/signals/live`, `/health` |
| `scripts/validate_edge.py` | Worker: Binance + Gamma → `logs/signals.csv` under `DATA_DIR` |
| `static/dashboard.html` | Control panel |
| `static/logo.png` | Lab logo (header + favicon via `/static/logo.png`) |
| `models/train.py` | Training; PKL under `models/saved/` |
| `models/saved/*.pkl` | Shipped for Railway/local without retraining |
| `strategies/` | Strategy manifests + experiment evidence (RUNBOOK / LEARNINGS / metrics) |

Train models before expecting ML columns in `logs/signals.csv`; without PKL files, NearRes and market logging still run.

---

## Active strategy index

| ID | Domain | Entry | Experiments |
|----|--------|--------|-------------|
| [crypto_5m_updown](strategies/crypto_5m_updown/) | crypto | `validate_edge.py` / `api.py` | [exogenous_compact](strategies/crypto_5m_updown/experiments/exogenous_compact/) |

---

## Committed experiment: exogenous_compact (crypto 5m)

Artifacts live under **one folder** so forks can audit numbers and narrative together:

| File | Role |
|------|------|
| [LEARNINGS.md](strategies/crypto_5m_updown/experiments/exogenous_compact/LEARNINGS.md) | Conclusions and interpretation |
| [RUNBOOK.md](strategies/crypto_5m_updown/experiments/exogenous_compact/RUNBOOK.md) | Commands to reproduce |
| [compact_feature_spec.md](strategies/crypto_5m_updown/experiments/exogenous_compact/compact_feature_spec.md) | Feature dictionary + anti-leakage |
| [compact_baseline_freeze.json](strategies/crypto_5m_updown/experiments/exogenous_compact/compact_baseline_freeze.json) | Saved model vs majority baseline |
| [compact_eval_report.md](strategies/crypto_5m_updown/experiments/exogenous_compact/compact_eval_report.md) / [`.json`](strategies/crypto_5m_updown/experiments/exogenous_compact/compact_eval_report.json) | LogReg baseline vs +exogenous (time-series CV) |
| [compact_go_no_go.json](strategies/crypto_5m_updown/experiments/exogenous_compact/compact_go_no_go.json) | Stricter GO/NO-GO gate |

Scripts `evaluate_compact_vs_baseline.py`, `freeze_baseline_compact.py`, and `compact_go_no_go.py` write here by default (override with `--experiment-dir` or `PM_STRATEGY_EXPERIMENT_DIR`).

---

## Local setup

```bash
python setup.py
source .venv/bin/activate
python download_datasets.py   # populates data/raw (gitignored)
python models/train.py
python scripts/validate_edge.py --hours 72
```

API + dashboard (optional):

```bash
AUTO_START=false PORT=8080 python scripts/api.py
```

Environment: `.env.example` → `.env` (`DATA_DIR`, `PORT`, `AUTO_START`, `SIGNAL_THRESHOLD`, `GAMMA_MAX_PAGES`, `VALIDATOR_HEALTH_PORT`, optional `PM_STRATEGY_EXPERIMENT_DIR`).

---

## Training target (important for any fork)

The supervised label in `models/train.py` is the **next** 5m candle being “green” in codebase terms:

**target:** `(close.shift(-1) >= open.shift(-1))` on the 5m frame.

Align any new strategy or offline study with this definition unless you deliberately change both train and serve paths.

---

## Adding a new strategy (sports / politics / other)

1. Copy `strategies/_template/` to `strategies/<your_id>/` and edit `strategy.yaml` (domain, data sources, runtime scripts—even if `null` at first).  
2. Create `strategies/<your_id>/experiments/<slug>/` with `RUNBOOK.md` and `LEARNINGS.md` **before** or **as soon as** you run analysis.  
3. Put only **small** artefacts in that folder (JSON, MD, CSV summaries). Keep downloads under `data/` or `strategies/<id>/data/` (gitignored patterns in `.gitignore`).  
4. Register the strategy in [strategies/README.md](strategies/README.md) index table and optionally `strategies/<id>/README.md`.

---

## Compact exogenous pipeline (crypto strategy)

Disk-light pull of **Binance Data Vision** UM aggregates + optional **Polymarket** `prices-history`, joined to spot 5m bars from 1m parquets.

1. `scripts/download_exogenous_compact.py` → `data/raw/exogenous/`  
2. `scripts/build_exogenous_features.py` → `data/raw/exogenous/compact_5m/{ASSET}.parquet`  
3. `scripts/evaluate_compact_vs_baseline.py`  
4. `scripts/freeze_baseline_compact.py`  
5. `scripts/compact_go_no_go.py`  

Findings snapshot (2026-04-27) is summarized in `LEARNINGS.md` above; raw data for reruns is not stored in git.

---

## Data policy (forks / CI)

- **`data/raw/`**, **`data/zips/`**, **`data/features/`** — gitignored.  
- **`logs/`** — gitignored.  
- **`reports/`** — local scratch / plots from exploration scripts (gitignored).  
- **`strategies/**/experiments/`** — commit MD/JSON; PNG/parquet/zip in experiments are gitignored (see `.gitignore`).  
- **`strategies/**/data/`**, **`scratch/`** — gitignored for per-strategy bulk.

---

## Deploy (Railway)

```bash
git push origin main
```

Set `PORT` in the service environment. Docker installs `libgomp1` for LightGBM; keep `scikit-learn` 1.6.x aligned with training for isotonic PKL compatibility (`pyproject.toml` / `Dockerfile`).

---

## Continuing this project

1. Read [CLAUDE.md](CLAUDE.md) for live worker/API behaviour.  
2. Read [strategies/README.md](strategies/README.md) for lab conventions.  
3. Re-download data, rerun pipelines, and **append** new experiment folders rather than overwriting committed evidence unless you intentionally supersede a run (consider a new `slug`).  
4. Keep train/serve feature names aligned with `models/train.py` and `validate_edge.py` for the default crypto stack.

Pull requests should not add raw datasets; add strategy metadata + RUNBOOK/LEARNINGS + compact metrics instead.
