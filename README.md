# PredMarket Arb

Statistical arbitrage for Polymarket 5-minute crypto markets.

The edge validator (`scripts/validate_edge.py`) logs structured messages to stderr (level from `LOG_LEVEL`) and updates a Rich live dashboard. Train models first so ML columns populate in `logs/signals.csv`; without PKL files, NearRes and market logging still run.

## Local Development

```bash
python setup.py
source .venv/bin/activate
python download_datasets.py
python models/train.py
python scripts/validate_edge.py --hours 72
```

## Deploy to Railway

```bash
git add .
git commit -m "deploy: validate_edge ready"
git push origin main
# Railway auto-deploys from GitHub
# View logs at railway.app/dashboard
```

Set `PORT` in the Railway service environment so the health check matches the listening port (the app reads `PORT` for the HTTP server).

## Files

- `scripts/api.py` — FastAPI dashboard + supervisor (`/` UI, `/health` JSON for Railway)
- `static/dashboard.html` — control panel (START/STOP, SSE live refresh)
- `scripts/validate_edge.py` — edge validator worker (subprocess desde el API)
- `scripts/healthcheck.py` — health HTTP del worker (`/health` en `VALIDATOR_HEALTH_PORT`)
- `models/saved/` — trained models (commit for Railway, ~26MB)
- `logs/signals.csv` — auto-generated signal log
- `Dockerfile` + `railway.toml` — deployment config

