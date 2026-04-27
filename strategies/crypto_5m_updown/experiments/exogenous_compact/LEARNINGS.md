# LEARNINGS — exogenous_compact

## Question

Do **compact** Binance UM-derived features (taker imbalance, trade counts, funding/OI aggregates) plus optional Polymarket mids, when joined to 5m spot bars, improve a **simple temporal baseline** (logistic regression on existing OHLC-derived features) enough to justify added complexity?

## Result (see JSON/MD in this folder)

- **Saved LightGBM + isotonic vs majority** (`compact_baseline_freeze.json`): small positive edge vs always predicting the majority class on BTC, ETH, SOL, XRP over the evaluated recent window; **BNB** slightly negative vs majority.
- **LogReg + exogenous vs baseline only** (`compact_eval_report.json` / `.md`): mixed by asset; the evaluation script can emit `GO_CANDIDATE` when compact “wins” on ≥60% of assets.
- **Stricter gate** (`compact_go_no_go.json`): requires ≥60% wins **and** mean CV delta ≥ **0.003** vs baseline → recorded decision **NO-GO** (mean delta slightly negative in the committed run).

## Interpretation

- Feature spec and leakage notes: `compact_feature_spec.md`.
- **Polymarket `prices-history`** coverage is **time-range and slug dependent**; for some historical windows used in a build, overlap with spot bars was **zero**—do not assume `poly_*` columns are always populated until you measure coverage on your window.
- Exogenous columns below coverage threshold are dropped per asset in eval (see script).

## Next steps

- Improve Polymarket slug/window alignment or drop `poly_*` until coverage is validated.
- Try alternative labels or horizons only if explicitly aligned with `models/train.py` live target to avoid train/serve skew.
- For a new hypothesis, add `strategies/<id>/experiments/<slug>/` with its own RUNBOOK + LEARNINGS rather than overloading this experiment.
