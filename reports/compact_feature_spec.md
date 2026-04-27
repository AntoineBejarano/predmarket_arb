# Compact Feature Spec (Anti-Leakage)

## Target
- `target = (close[t+1] >= open[t+1])`

## Reglas anti-leakage
- Todas las features en fila `t` usan información disponible hasta cierre de vela `t`.
- Prohibido usar cualquier campo de vela `t+1` en features.
- Features lentas (funding) sólo por `ffill` hacia delante desde último valor conocido.
- Features de Polymarket se guardan con timestamp real de captura y nunca se adelantan en el tiempo.

## Features baseline (existentes)
- `ret_1`, `ret_3`, `ret_6`, `ret_12`, `ret_24`, `ret_48`
- `vol_10`, `vol_ratio`, `atr_5`, `vol_zscore`, `vol_trend`
- `hour`, `dow`, `is_ny_open`

## Features exógenas MVP (compactas)
- `taker_buy_qty`, `taker_sell_qty`
- `taker_buy_notional`, `taker_sell_notional`
- `n_trades`
- `taker_imbalance_5m = (buy_qty - sell_qty) / (buy_qty + sell_qty)`
- `buy_sell_ratio_5m = buy_qty / sell_qty`
- `sum_open_interest`, `sum_open_interest_value`
- `oi_change_5m`
- `sum_taker_long_short_vol_ratio`
- `funding_rate`, `funding_change`
- `poly_mid`, `poly_mid_change_1`
- `spot_vs_poly_gap = close - poly_mid`

## Features excluidas por riesgo de leakage
- Cualquier precio final de mercado Polymarket posterior al cierre de ventana objetivo.
- Variables derivadas explícitamente de `close[t+1]`, `open[t+1]`, `high[t+1]`, `low[t+1]`.
