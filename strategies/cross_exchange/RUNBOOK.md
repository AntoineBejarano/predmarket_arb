# Cross-Exchange Arbitrage — RUNBOOK

## Principio matemático

El mismo evento está priceado en Polymarket (CLOB descentralizado) y Kalshi
(exchange regulado CFTC). Las bases de usuarios difieren, generando gaps.

Arbitraje: si P_poly(YES) + P_kalshi(NO) + fees < 1.00
  → comprar YES en Poly, comprar NO en Kalshi
  → ambas posiciones se cubren mutuamente, profit = 1 - (P_yes + P_no + fees)

Fórmula neta:

- spread_gross = 1.0 - poly_best_ask(YES) - kalshi_best_ask(NO)
- fees_total = POLY_FEE_RATE + KALSHI_FEE_RATE
- edge = spread_gross - fees_total
- Si edge > MIN_EDGE → SEÑAL

## Mapeo de mercados

Mantener un archivo `data/poly_kalshi_mapping.csv` con:
  poly_market_id, kalshi_ticker, event_description, expires_at

Este archivo se construye manualmente o con el script
`scripts/build_market_mapping.py` (por crear en Fase 2).

## Datos de entrada

- Polymarket CLOB REST: GET /orderbook/{token_id} → best_ask YES
- Kalshi REST: GET /trade-api/v2/markets/{ticker}/orderbook → yes_ask, no_ask
- Polling interval: cada 30 segundos (rate limit Kalshi: 100 req/min)

## CSV schema

| campo | tipo | descripción |
|-------|------|-------------|
| ts | ISO8601 | timestamp UTC |
| poly_market_id | str | ID mercado Polymarket |
| kalshi_ticker | str | ticker Kalshi |
| poly_yes_ask | float | best ask YES en Poly |
| kalshi_no_ask | float | best ask NO en Kalshi |
| spread_gross | float | 1 - poly_ask - kalshi_ask |
| fees_total | float | fees combinados |
| edge | float | spread_gross - fees_total |
| lockup_days | int | días hasta resolución |
| action | str | SIGNAL / SKIP / EXECUTED / DRY_RUN |
| dry_run | bool | paper o real |

## Riesgo principal: lock-up

El capital queda inmovilizado hasta resolución. Solo ejecutar en contratos
con resolución < 21 días. Trackear en `logs/lockup_tracker.csv`.

## Parámetros (env vars)

- CROSS_MIN_EDGE=0.030
- CROSS_MAX_SIZE_USDC=200
- CROSS_MAX_LOCKUP_DAYS=21
- CROSS_POLL_INTERVAL=30
- KALSHI_API_KEY= (requerido para órdenes, opcional para lectura)
