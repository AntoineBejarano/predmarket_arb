# RUNBOOK — poly_aligned_target

Experimento que alinea el target de entrenamiento con resoluciones reales de Polymarket.

## Precondiciones

```bash
python setup.py && source .venv/bin/activate
python download_datasets.py   # Binance 1min desde 2023-01-01 (piso de fechas; parquet viejos no se truncan solos)
```

Si necesitas historial Binance **solo** desde 2023, borra o regenera `data/raw/{ASSET}_1min.parquet` antes de descargar.

## Pipeline

```bash
# 1. Descargar mercados Polymarket resueltos
python scripts/download_poly_history.py

# 2. Construir dataset alineado por timestamp
python scripts/build_poly_target.py

# 3. Reentrenar con nuevo target y features
python models/train.py --force
```

## Outputs esperados

- `data/raw/poly_markets_resolved.parquet`
- `data/raw/poly_aligned/{ASSET}.parquet` para cada activo
- `models/saved/{ASSET}_model.pkl` (reentrenados)
- `models/saved/{ASSET}_calibrator.pkl` (reentrenados)
- `models/training_report.json` con campos `poly_rows` y `target_source`

## Despliegue validador (fase posterior)

Los `.pkl` nuevos usan **17 features**. `scripts/validate_edge.py` sigue con la lista corta (~14): **no** uses estos modelos en el validador en vivo hasta alinear features allí.

## Go/No-Go

Criterio de éxito: edge medio en walk-forward >= 0.02 sobre mayoría,
con `target_source` = `poly+binance` en al menos BTC y ETH.

Si edge < 0.015, revisar cobertura de `poly_aligned` antes de proceder
al pipeline de ejecución.
