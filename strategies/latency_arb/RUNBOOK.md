# Latency Arbitrage — RUNBOOK

## Principio matemático

Polymarket actualiza sus precios más lento que los CEX. Cuando el precio
de BTC/ETH se mueve bruscamente en Binance Futures, existe una ventana
de ~100-500ms donde los mercados de Polymarket relacionados no han
actualizado todavía.

Señal:

- delta_pct = |price_now - price_prev| / price_prev
- Si delta_pct > TICK_THRESHOLD → explorar mercados relacionados en Poly
- Si precio implícito de Poly sigue siendo compatible con el estado ANTERIOR
  de Binance → ejecutar en dirección del nuevo precio

## Mercados relacionados

Mantener `data/binance_poly_mapping.json`:

```json
{
  "BTCUSDT": {
    "up_markets": ["market_id_btc_above_X", ...],
    "down_markets": ["market_id_btc_below_X", ...]
  }
}
```

## CSV schema

| campo | tipo | descripción |
|-------|------|-------------|
| ts | ISO8601 | |
| symbol | str | BTCUSDT / ETHUSDT |
| price_trigger | float | precio de Binance que disparó la señal |
| delta_pct | float | movimiento porcentual |
| poly_market_id | str | mercado de Poly explotado |
| poly_price_before | float | precio Poly antes de la señal |
| poly_price_after | float | precio Poly después (para medir ventana) |
| latency_ms | int | ms desde señal Binance hasta orden Poly |
| action | str | SIGNAL / SKIP / EXECUTED / DRY_RUN |
| edge_est | float | edge estimado en el momento de ejecución |
| dry_run | bool | |

## Parámetros

- LAT_TICK_THRESHOLD=0.003   (0.3% movimiento mínimo en Binance)
- LAT_MAX_SIZE_USDC=150      (posiciones pequeñas, alta rotación)
- LAT_MAX_LATENCY_MS=400     (si ya tardó más de 400ms, skip)
- LAT_SYMBOLS=BTCUSDT,ETHUSDT
