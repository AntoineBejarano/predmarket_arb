# Market Making Avellaneda-Stoikov — RUNBOOK

## Principio matemático

El modelo AS calcula el precio de reserva (reservation price) y el spread
óptimo considerando el inventario actual y la volatilidad del mercado.

Parámetros del modelo:

- q = inventario actual (número de contratos en posesión, signed)
- σ = volatilidad del mid price (ventana deslizante de 1h)
- γ = aversión al riesgo (0.1 por defecto, calibrar)
- k = tasa de llegada de órdenes (estimado de fills por hora)
- T = tiempo hasta expiración del mercado (en horas)
- t = tiempo actual

Fórmulas:

- r = mid - q * γ * σ² * (T - t)           # reservation price
- δ = γ * σ² * (T - t) / 2 + ln(1 + γ/k) / γ   # half-spread óptimo
- bid_quote = r - δ
- ask_quote = r + δ

## Target de mercados

Solo mercados con:

- bid-ask spread actual > 4 céntimos (0.04)
- volumen diario entre $1,000 y $30,000
- tiempo a expiración > 24 horas
- NO mercados con < $500 de liquidez total

## Calibración de σ

Calcular en vivo desde snapshots del mid price almacenados en
`logs/market_maker_prices.csv` (ring buffer de 60 observaciones × 1min).
σ = std(mid_prices[-60:]) en términos de movimiento porcentual.

## CSV schema

| campo | tipo | descripción |
|-------|------|-------------|
| ts | ISO8601 | timestamp UTC |
| market_id | str | ID mercado |
| mid | float | mid price actual |
| q_inventory | int | inventario actual (contratos) |
| sigma | float | volatilidad estimada |
| reservation_price | float | precio de reserva AS |
| bid_quote | float | bid calculado |
| ask_quote | float | ask calculado |
| bid_placed | float | bid que se colocó (o null) |
| ask_placed | float | ask que se colocó (o null) |
| action | str | QUOTE / SKIP / CANCEL / FILL |
| dry_run | bool | |

## Parámetros (env vars)

- MM_GAMMA=0.1
- MM_K=2.0              (fills por hora, recalibrar con datos reales)
- MM_MIN_SPREAD=0.04    (spread mínimo target de mercado)
- MM_MAX_INVENTORY=50   (contratos, stop colocar si q > max)
- MM_QUOTE_INTERVAL=30  (segundos entre requote)
- MM_TARGET_MARKETS=    (CSV de market_ids a makerear, vacío = auto-detect)
