# Combinatorial Arbitrage — RUNBOOK

## Principio matemático

Existen dos tipos (arXiv 2508.03474):

**Tipo 1 — Market Rebalancing (intra-market):**
Dentro de un mercado de N outcomes, la suma puede desviarse de 1.00
por asimetría en el book. Idéntico al Bundle Arb pero con legs
sintéticas (short de outcomes sobrevalorados cuando es posible).

**Tipo 2 — Combinatorial (cross-market):**
Cuando dos mercados tienen outcomes lógicamente relacionados:
  P(A ocurre) y P(B ocurre) y P(A Y B ocurren)
  La consistencia exige: P(A∧B) ≤ min(P(A), P(B))
  Si P(A∧B) > P(A) → comprar A, vender el combinado (si existe mercado)

## Grafo de relaciones

Construir un grafo en `data/market_graph.json`:
  Nodos: market_id
  Aristas: relación lógica (subset, superset, complement, independent)

Este grafo se actualiza con `scripts/build_market_graph.py` (por crear).
Heurística inicial: mercados con mismo subyacente (BTC, ETH) y fechas
solapadas tienen alta probabilidad de relación lógica.

## CSV schema


| campo      | tipo    | descripción                             |
| ---------- | ------- | --------------------------------------- |
| ts         | ISO8601 |                                         |
| arb_type   | str     | REBALANCING / COMBINATORIAL             |
| markets    | str     | JSON array de market_ids involucrados   |
| legs       | str     | JSON de {token_id, side, price} por leg |
| cost_total | float   | coste total de abrir todas las legs     |
| fair_value | float   | pago garantizado si hay arb             |
| edge       | float   | fair_value - cost_total - fees          |
| action     | str     | SIGNAL / SKIP / EXECUTED / DRY_RUN      |
| dry_run    | bool    |                                         |


## Parámetros

- COMBO_MIN_EDGE=0.030
- COMBO_MAX_LEGS=4
- COMBO_MAX_SIZE_USDC=400
- COMBO_SCAN_INTERVAL=60  (segundos, más lento — operación costosa)