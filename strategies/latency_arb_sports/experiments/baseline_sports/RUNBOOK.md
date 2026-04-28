# RUNBOOK — baseline_sports (`latency_arb_sports`)

## Objetivo

Paper / laboratorio: comparar probabilidad fair Pinnacle (The Odds API) con mid CLOB Polymarket en mercados deportivos Gamma, con matching explícito y TTL de descubrimiento anti-ban.

## Variables de entorno

- **`ENABLE_EXPERIMENTAL`**: por defecto en `arb_engine.py` es **activo** (`true`) para no depender de variables en Railway. Solo si defines `ENABLE_EXPERIMENTAL=false` el motor queda en 3 estrategias base y **no** cargará `latency_arb_sports` aunque el toggle en la UI esté ON.
- Con el motor en marcha, en Deploy Logs verás líneas `[latency_arb_sports] cycle #…` con conteos de Gamma, Odds API y matches (solo si esta estrategia está cargada).
- `DRY_RUN=true` (recomendado) — no envía órdenes; `false` + credenciales L2 para POST `/order` FOK.
- `LATENCY_SPORTS_MIN_EDGE`, `LATENCY_SPORTS_SPORTS`, `LATENCY_SPORTS_POLL_INTERVAL`, `LATENCY_SPORTS_MAX_STAKE_USDC`, `LATENCY_SPORTS_REGIONS` — ver `.env.example`.
- `LATENCY_SPORTS_DISCOVERY_TTL=300` — default y **mínimo** 300 s (clamp en runtime); no bajar para no martillar Gamma.
- `ODDS_API_KEY` — preferido sobre clave LAB en código.
- `LATENCY_SPORTS_GAMMA_TAG_ID` — opcional; si Gamma lo soporta en `/markets`, filtra por `tag_id`.

## Reglas de match (Gamma ↔ Odds API)

1. Mismo `sport_key` (Odds) inferido en Gamma por keywords (`SPORT_KEYWORDS` en `arb/latency_arb_sports.py`).
2. Equipos: `teams_match_odds_gamma` (Levenshtein &lt; 3 y contención / última palabra) en home y away; se aceptan permutaciones home/away.
3. Tiempo: `abs(commence_gamma - commence_odds) < 3600` s UTC, donde `commence_gamma` es el **más cercano** entre texto `scheduled for`, `endDate` y `startDate` del mercado Gamma.

## Arranque

```bash
source .venv/bin/activate
export ENABLE_EXPERIMENTAL=true
export DRY_RUN=true
python scripts/arb_engine.py
```

O desde el API: Motor Arb → Start; activar toggle `latency_arb_sports` en `/arb` o en `/arb/strategy/latency_arb_sports`.

## CSV

Ruta: `$DATA_DIR/logs/latency_arb_sports.csv` (columnas `ts`, `action`, `reason`, … + `edge`, `status`).

## UI

- Índice: `/arb`
- Detalle: `/arb/strategy/latency_arb_sports` (`static/latency_sports.html`)

## Interpretación de señales

- `action=SIGNAL` / `EXECUTED`: oportunidad con `edge` positivo (fair − mid) por encima de `LATENCY_SPORTS_MIN_EDGE`.
- `SKIP:LOW_EDGE` / `SKIP:CIRCUIT_BREAKER`: sin trade o breaker activo.
