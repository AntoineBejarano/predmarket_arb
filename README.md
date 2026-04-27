# PredMarket Arb

Paper-trading / edge-validation stack for Polymarket-style **5-minute crypto Up or Down** markets. It compares market-implied probabilities (Gamma API) with an ML model (LightGBM + isotonic calibration) and a NearRes heuristic. **There is no trading or wallet**—only public API reads and CSV logs.

For deploy and day-to-day ops notes (Spanish), see [`CLAUDE.md`](CLAUDE.md).

---

## What you get in this repo

| Piece | Role |
|--------|------|
| `scripts/api.py` | FastAPI: dashboard at `/`, JSON `/api/status`, START/STOP worker, SSE `/api/signals/live`, `/health` for Railway |
| `scripts/validate_edge.py` | Long-running worker: Binance + Gamma, writes `logs/signals.csv` under `DATA_DIR` |
| `static/dashboard.html` | Vanilla UI + Tailwind CDN |
| `models/train.py` | Training pipeline; saves PKL under `models/saved/` |
| `models/saved/*.pkl` | Committed so Railway/local can run the validator without retraining |

Train models before expecting ML columns in `logs/signals.csv`; without PKL files, NearRes and market logging still run.

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
# http://127.0.0.1:8080/  —  http://127.0.0.1:8080/health
```

Environment: copy `.env.example` → `.env`. Important variables include `DATA_DIR`, `PORT`, `AUTO_START`, `SIGNAL_THRESHOLD`, `GAMMA_MAX_PAGES`, `VALIDATOR_HEALTH_PORT`.

---

## Training target (important for any fork)

The supervised label in `models/train.py` is the **next** 5m candle being “green” in the sense used in the codebase:

**`target = (close.shift(-1) >= open.shift(-1))`**

i.e. whether the **following** bar’s close is at or above that bar’s open—not a 15m horizon or same-bar direction unless you change the code. Any offline analysis or new features should stay aligned with this definition to avoid silent train/serve skew.

---

## Compact exogenous experiment (completed, documented only)

A **disk-light** pipeline was added to pull **Binance Data Vision** UM aggregates (aggTrades → 5m taker imbalance / trade counts, daily OI metrics, monthly funding) and optional **Polymarket** `prices-history` samples, then join them to spot 5m bars built from the existing 1m parquet pipeline.

**Scripts (run order after you have `data/raw/{ASSET}_1min.parquet` from `download_datasets.py`):**

1. `scripts/download_exogenous_compact.py` — writes under `data/raw/exogenous/` (large; never commit).
2. `scripts/build_exogenous_features.py` — outputs `data/raw/exogenous/compact_5m/{ASSET}.parquet`.
3. `scripts/evaluate_compact_vs_baseline.py` — logistic regression + time-series CV; writes `reports/compact_eval_report.{json,md}`.
4. `scripts/freeze_baseline_compact.py` — compares **saved** model vs majority baseline on recent history → `reports/compact_baseline_freeze.json`.
5. `scripts/compact_go_no_go.py` — conservative gate from the eval JSON → `reports/compact_go_no_go.json`.

Feature definitions and anti-leakage notes: `reports/compact_feature_spec.md`.

### Findings (snapshot from committed reports, 2026-04-27)

- **Saved model vs majority** (last ~6 months of 5m bars, same target as training): small positive edge on BTC/ETH/SOL/XRP; **BNB slightly negative** vs majority. See `reports/compact_baseline_freeze.json`.
- **Exogenous “compact” features vs logistic baseline** on the evaluated window: mixed per asset; `evaluate_compact_vs_baseline.py` may label a run `GO_CANDIDATE` when compact wins a 60% majority of assets, but **`compact_go_no_go.json` applies a stricter rule** (≥60% wins **and** mean CV delta ≥ **0.003**). With that rule the recorded decision is **NO-GO** (mean delta slightly negative). See `reports/compact_go_no_go.json`.
- **Polymarket history**: API responses depend on slug and time range; for some historical windows the join had **no overlapping Polymarket mid** with the spot window used in one build—treat `poly_*` columns as optional and validate coverage before relying on them.

Raw downloaded data for this experiment was **removed from the workspace** on purpose; reproduce by re-running the download scripts after `download_datasets.py`.

---

## Data policy (forks / CI)

- **`data/raw/`, `data/zips/`, `data/features/`** are gitignored—do not commit large parquets or zips.
- **`logs/`** is gitignored.
- **`reports/`**: bulky exploration outputs (e.g. PNG from `scripts/explore_data.py`) stay local. **Committed** artefacts are only the compact analysis files listed in `.gitignore` negation rules (`compact_*`, `compact_feature_spec.md`).

---

## Deploy (Railway)

```bash
git push origin main
# Railway auto-deploys from GitHub; health: GET /health on the API
```

Set `PORT` in the service environment to match the listening port. Docker image installs `libgomp1` for LightGBM; keep `scikit-learn` in the same 1.6.x family as training for isotonic PKL compatibility (see `pyproject.toml` / `Dockerfile`).

---

## Continuing this project

1. Read `CLAUDE.md` for process architecture and log interpretation.
2. Re-download data with `download_datasets.py`, then optional exogenous scripts above.
3. Use `reports/compact_*` as the baseline narrative for the exogenous experiment; extend with new features, targets, or walk-forward design as needed.
4. Keep train/serve feature names aligned with `models/train.py` and the live validator.

Pull requests should **not** add raw datasets; attach methodology and numbers in `reports/` or short markdown only when it helps reviewers.
