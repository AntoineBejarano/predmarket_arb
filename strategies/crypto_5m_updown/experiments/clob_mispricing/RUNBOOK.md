# RUNBOOK — clob_mispricing

Estrategia: detectar mispricing entre modelo ML (Binance) y CLOB Polymarket.

## Hipótesis

El modelo LightGBM entrenado sobre velas Binance predice P(BTC sube en 5m).
El CLOB de Polymarket pone precio al token YES (prob implícita del mercado).
Cuando |modelo - mercado| >= 5%, hay edge explotable.

## Pipeline de ejecución (6 ciclos)

1. Scan: descubrir mercados BTC 5m activos via Gamma API
2. Predict: modelo LightGBM → P(up) calibrada
3. Validate: CLOBSignalFilter.evaluate() — edge >= 5%, liquidez >= 50 USDC
4. Size: Kelly fraccionario sobre el edge detectado
5. Fill: POST /order al CLOB (DRY_RUN=true por defecto)
6. Settle: tracking de resolución y PnL

## Entrenamiento

```bash
python models/train.py --asset BTC --force
```

## Validación del filtro

```bash
python scripts/clob_signal_filter.py
```

## Variables de entorno clave

- DRY_RUN=true (default seguro)
- MIN_EDGE=0.05
- MIN_LIQUIDITY_USDC=50

## Go/No-Go

- Edge medio en walk-forward Binance >= 0.02
- CLOBSignalFilter filtra correctamente casos de prueba
- Paper trading 1 semana con DRY_RUN=true antes de capital real
