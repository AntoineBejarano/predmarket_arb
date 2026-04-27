# Strategy laboratory

This repository is organized as a **lab**: each **strategy** is a hypothesis about Polymarket (or cross‑market) edge. Strategies can target **crypto**, **sports**, **politics**, or anything else you can resolve to structured data + market prices.

## Layout

```text
strategies/
  README.md                 ← you are here
  _template/                ← copy to bootstrap a new strategy
    strategy.yaml
    experiments/_template/
      RUNBOOK.md            ← exact commands, data versions, git SHA when you ran
      LEARNINGS.md          ← narrative: what worked, what failed, next steps
  <strategy_id>/
    strategy.yaml           ← metadata: domain, Polymarket family, runtime entrypoints
    README.md               ← short pitch for this strategy (optional)
    experiments/
      <experiment_slug>/    ← one reproducible “run” or paper (immutable-ish)
        RUNBOOK.md
        LEARNINGS.md
        *.json / *.md       ← metrics, specs (small files only; commit to git)
```

**Rules of thumb**

1. **Never commit** raw downloads, large parquet, or notebooks outputs—use `data/` (gitignored) or `strategies/<id>/data/` for strategy-specific scratch (also gitignored).
2. **Always commit** for each experiment: `RUNBOOK.md` (how to reproduce), `LEARNINGS.md` (what you concluded), and compact metrics/spec JSON or Markdown.
3. **Strategy ID** = lowercase slug, stable over time (`crypto_5m_updown`, `nba_spreads_v1`, …).
4. **Experiment slug** = short descriptor + optional date (`exogenous_compact`, `walkforward_2026q1`).

## Runtime vs research

| Layer | Role |
|--------|------|
| `scripts/validate_edge.py` + `scripts/api.py` | Today: **live paper** for the default crypto 5m stack (Railway). |
| `models/train.py` | Training for that stack; new strategies may add their own train scripts under `scripts/` or `strategies/<id>/`. |
| `strategies/*/experiments/*` | **Evidence store**: what you ran, what you learned. |

New domains (sports/politics) will usually add **new** validators or batch jobs under `scripts/` while reusing shared clients (Gamma, CLOB) where possible—keep entrypoints declared in `strategy.yaml`.

## Index

| Strategy ID | Domain | Description |
|-------------|--------|-------------|
| [crypto_5m_updown](crypto_5m_updown/) | crypto | 5m spot vs Polymarket Up/Down; ML + NearRes + compact exogenous experiment. |
