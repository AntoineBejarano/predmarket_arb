# Bundle Arbitrage — RUNBOOK

## Principio matemático

Cada mercado de Polymarket tiene N outcomes mutuamente excluyentes y exhaustivos.
La suma de sus probabilidades debe ser exactamente 1.00.
Cuando ∑ best_ask(outcome_i) < 1.00, existe un arbitraje libre de riesgo:
comprar todos los outcomes garantiza un pago de $1.00 con coste < $1.00.

Fórmula:

- edge = 1.00 - ∑ best_ask(i) - gas_estimate
- Si edge > MIN_EDGE (default 0.025) → SEÑAL

## Datos de entrada

- Polymarket CLOB REST: GET /markets → lista de mercados activos
- Polymarket CLOB REST: GET /orderbook/{token_id} → best_ask por outcome
- Polling interval: cada 10 segundos por mercado
- Priorizar mercados con 2-3 outcomes (más frecuente la ineficiencia)

## Lógica de ejecución

```text
for each active_market:
    outcomes = get_outcomes(market_id)
    prices = [get_best_ask(token_id) for token_id in outcomes]
    total = sum(prices)
    gas = 0.012 * len(outcomes)   # estimado Polygon, en USDC
    edge = 1.0 - total - gas
    if edge > MIN_EDGE:
        log_signal(market_id, outcomes, prices, edge)
        if not DRY_RUN:
            for outcome in outcomes:
                place_order(outcome.token_id, "buy", outcome.best_ask)
```

## CSV schema


| campo      | tipo    | descripción                               |
| ---------- | ------- | ----------------------------------------- |
| ts         | ISO8601 | timestamp UTC                             |
| market_id  | str     | ID del mercado Polymarket                 |
| n_outcomes | int     | número de outcomes                        |
| sum_ask    | float   | suma de best_ask de todos los outcomes    |
| gas_est    | float   | coste estimado de gas en USDC             |
| edge       | float   | 1.0 - sum_ask - gas_est                   |
| action     | str     | SIGNAL / SKIP / EXECUTED / DRY_RUN        |
| size_usdc  | float   | tamaño de la posición en USDC (0 si SKIP) |
| dry_run    | bool    | si fue ejecutado en paper o real          |


## Riesgos

- Resolución ambigua: leer siempre market.rules antes de ejecutar
- Gas spike en Polygon: si gas > 0.05 USDC/tx, saltar la oportunidad
- Ventana corta: bots institucionales escanean esto. Edge real en mercados
con < $50K volumen total o abiertos hace < 6 horas.

## Parámetros (env vars)

- BUNDLE_MIN_EDGE=0.025 (mínimo 2.5% de edge)
- BUNDLE_MAX_SIZE_USDC=300 (máximo por trade)
- BUNDLE_POLL_INTERVAL=10 (segundos entre scans)
- BUNDLE_MAX_OUTCOMES=5 (descartar mercados con más de N outcomes)

