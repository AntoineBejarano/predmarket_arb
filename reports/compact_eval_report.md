# Compact Plan Evaluation

Comparación baseline vs baseline+exógenas (LogReg + TimeSeriesSplit).

| Asset | Rows | Majority | Baseline mean±std | Compact mean±std | Delta vs baseline |
| --- | ---: | ---: | ---: | ---: | ---: |
| BTCUSDT | 26,496 | 0.5014 | 0.5094±0.0040 | 0.5107±0.0067 | +0.0013 |
| ETHUSDT | 26,496 | 0.5058 | 0.5196±0.0095 | 0.5171±0.0058 | -0.0025 |
| SOLUSDT | 26,496 | 0.5119 | 0.5148±0.0066 | 0.5161±0.0071 | +0.0013 |
| XRPUSDT | 26,496 | 0.5254 | 0.5251±0.0105 | 0.5269±0.0082 | +0.0019 |
| BNBUSDT | 26,496 | 0.5270 | 0.5200±0.0050 | 0.5156±0.0032 | -0.0043 |

## Decision
GO_CANDIDATE

## Success Criteria
- Mejora consistente y robusta sobre baseline.
- Edge medio objetivo >= 0.02 sobre mayoría en validación temporal.
