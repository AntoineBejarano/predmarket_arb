# Term Structure Arbitrage — RUNBOOK

## Principio matemático

El mismo evento subyacente existe en múltiples contratos con fechas de
expiración diferentes. Ejemplo: "¿BTC > 100k antes del 30 jun?" y
"¿BTC > 100k antes del 31 dic?".

Si el precio de ambos contratos es P1 (corto) y P2 (largo):
  La consistencia exige P2 ≥ P1 (más tiempo = más probabilidad de ocurrir)
  Si P2 < P1 → mispricing → comprar P2, (sintéticamente) vender P1

Modelo de Poisson para calibración:
  P(evento antes de T) = 1 - e^(-λ * T)
  λ se calibra con datos históricos de resoluciones de eventos similares
  almacenados en `data/resolution_history.csv`

Señal:

- p_model_1 = 1 - exp(-lambda_hat * T1_hours)
- p_model_2 = 1 - exp(-lambda_hat * T2_hours)
- mispricing_1 = abs(market_price_1 - p_model_1) / p_model_1
- mispricing_2 = abs(market_price_2 - p_model_2) / p_model_2
- Si (mispricing_1 + mispricing_2) / 2 > MIN_MISPRICING → SEÑAL

## CSV schema

| campo | tipo | descripción |
|-------|------|-------------|
| ts | ISO8601 | |
| market_id_short | str | contrato con expiración más próxima |
| market_id_long | str | contrato con expiración más lejana |
| price_short | float | precio mercado del contrato corto |
| price_long | float | precio mercado del contrato largo |
| model_short | float | precio modelo Poisson contrato corto |
| model_long | float | precio modelo Poisson contrato largo |
| lambda_hat | float | λ calibrado |
| mispricing | float | desviación promedio |
| action | str | SIGNAL / SKIP / EXECUTED / DRY_RUN |
| dry_run | bool | |

## Parámetros

- TERM_MIN_MISPRICING=0.15   (15% de desviación del modelo)
- TERM_MAX_SIZE_USDC=300
- TERM_LAMBDA_WINDOW=30      (días de historial para calibrar λ)
- TERM_SCAN_INTERVAL=300     (segundos, cada 5 min es suficiente)
