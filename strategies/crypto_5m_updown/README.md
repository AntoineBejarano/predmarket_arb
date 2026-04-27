# Strategy: crypto_5m_updown

**Domain:** crypto  
**Polymarket:** recurring 5-minute-style crypto Up/Down markets (filtered via Gamma in `validate_edge.py`).  
**Edge stack:** Binance spot → synthetic 5m bars → LightGBM + isotonic calibrators in `models/saved/` → paper log `logs/signals.csv`.

## Experiments (evidence)

| Slug | Summary |
|------|---------|
| [exogenous_compact](experiments/exogenous_compact/) | Compact exogenous futures/order-flow features + logistic CV vs baseline; baseline freeze vs saved model; go/no-go gate. |

Add a new row here when you create `experiments/<new_slug>/`.
