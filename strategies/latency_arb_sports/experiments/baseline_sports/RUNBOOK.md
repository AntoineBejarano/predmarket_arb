# RUNBOOK — baseline_sports (`latency_arb_sports`)

## Objetivo

Paper / laboratorio: comparar probabilidad fair Pinnacle (The Odds API) con mid CLOB Polymarket en mercados deportivos Gamma, con matching explícito y TTL de descubrimiento anti-ban.

## Variables de entorno

- **`ENABLE_EXPERIMENTAL`**: por defecto en `arb_engine.py` es **activo** (`true`) para no depender de variables en Railway. Solo si defines `ENABLE_EXPERIMENTAL=false` el motor queda en 3 estrategias base y **no** cargará `latency_arb_sports` aunque el toggle en la UI esté ON.
- Con el motor en marcha, en Deploy Logs verás líneas `[latency_arb_sports] cycle #…` con conteos de **public-search** (HTTP vs aciertos de caché TTL), Odds API y matches (solo si esta estrategia está cargada).
- `DRY_RUN=true` (recomendado) — no envía órdenes; `false` + credenciales L2 para POST `/order` FOK.
- `LATENCY_SPORTS_MIN_EDGE`, `LATENCY_SPORTS_SPORTS`, `LATENCY_SPORTS_POLL_INTERVAL`, `LATENCY_SPORTS_MAX_STAKE_USDC`, `LATENCY_SPORTS_REGIONS` — ver `.env.example`.
- `LATENCY_SPORTS_DISCOVERY_TTL=300` — default y **mínimo** 300 s (clamp en runtime); no bajar para no martillar Gamma. El resultado de **public-search por partido** se cachea con este TTL (no se repite el GET cada ciclo de poll para el mismo evento Odds).
- `ODDS_API_KEY` — preferido sobre clave LAB en código.
- `LATENCY_SPORTS_GAMMA_PUBLIC_SEARCH_LIMIT` — límite de eventos pedidos a `GET /public-search` (default 40).

## Gamma: tags y discovery

En Gamma, los **tags no son “deporte genérico”** (no hay un tag estable equivalente a `soccer_epl` de Odds). Los `tag_id` / `tag_slug` suelen ir **por competición concreta** (IDs o slugs tipo liga o torneo). Por eso esta estrategia **no** depende de mapear `sport_key` → tag Gamma.

**Discovery actual:** por cada partido Odds (Pinnacle h2h), se llama a `GET {GAMMA}/public-search?q={home}+{away}` y se filtran eventos cuyo `title`/`slug` contengan **ambos** equipos vía `teams_match_odds_gamma` (misma heurística Levenshtein que el match fino en mercados).

## Reglas de match (Gamma ↔ Odds API)

1. **Deporte:** el `sport_key` solo etiqueta el bucket Odds (EPL, NBA, …); el enlace a Gamma es por **texto de equipos + ventana temporal**, no por inferencia de deporte en mercados Gamma.
2. Equipos: `teams_match_odds_gamma` en home y away frente al blob `title`+`slug` del evento Gamma (y en outcomes del mercado hijo al construir la fila CLOB).
3. Tiempo: `abs(commence_gamma - commence_odds) < 3600` s UTC, donde `commence_gamma` es el **más cercano** entre texto `scheduled for`, `endDate` y `startDate` del mercado Gamma elegido dentro del evento.

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
