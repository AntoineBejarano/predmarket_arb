# PredMarket Arb — guía para asistentes (Claude / Cursor)

## Qué es este repo

Sistema de **paper trading / validación de edge** sobre mercados Polymarket tipo “5m crypto Up or Down”. Compara probabilidad del mercado (Gamma API) con un modelo ML (LightGBM + isotónica) y una heurística NearRes. **No hay trades ni wallet**: solo lectura de APIs y logs en CSV.

## Arquitectura en dos procesos


| Proceso                        | Rol                                                                                                                                                                  |
| ------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `**scripts/api.py`**           | FastAPI: dashboard en `/` (`static/dashboard.html`), JSON `/api/status`, START/STOP del worker, SSE `/api/signals/live`, `/health` → `{"status":"ok"}` para Railway. |
| `**scripts/validate_edge.py`** | Worker largo: Binance + Gamma, Rich en consola, escribe `logs/signals.csv`. Arrancado por el API como subproceso o en local a mano.                                  |


- Comunicación: `**logs/signals.csv**` (ruta bajo `DATA_DIR`) y PID en memoria del API.
- **Puertos:** el API usa `PORT` (p. ej. 8080). El worker tiene su propio mini HTTP de health en `**VALIDATOR_HEALTH_PORT`** (default `18088`) para no colisionar con el API.

## Archivos importantes

- `models/train.py` — entrena y guarda PKL en `models/saved/{BTCUSDT,...}_*.pkl`. Las features en vivo del validador están alineadas con este pipeline (velas ~5m sintéticas desde ticks de 30s).
- `scripts/validate_edge.py` — bucle principal, resolución de ventanas, informes diarios UTC, `SIGNAL_THRESHOLD`, etc.
- `scripts/healthcheck.py` — handler HTTP mínimo usado por el worker.
- `static/dashboard.html` — UI vanilla + Tailwind CDN; sin build npm.
- `Dockerfile` / `railway.toml` — arranque con `python scripts/api.py`; healthcheck Railway en `/health` del API.

En **`python:3.11-slim`** hace falta el paquete **`libgomp1`** (OpenMP) o LightGBM falla al cargar PKL: `libgomp.so.1: cannot open shared object file` — ya instalado en el `Dockerfile`.

## Entorno y variables

- Copiar `.env.example` → `.env`. Relevantes: `DATA_DIR`, `PORT`, `AUTO_START`, `SIGNAL_THRESHOLD`, `LOG_LEVEL`, `GAMMA_MAX_PAGES`, `VALIDATOR_HEALTH_PORT`, `DASHBOARD_PASSWORD` (reservado; auth no implementada).
- Python del proyecto: `**pyproject.toml` pide ≥3.11** (Docker); entornos locales pueden ser 3.9 — en rutas FastAPI evitar anotaciones `X | Y` sin `from __future__ import annotations` o usar `Union`/`Optional` donde FastAPI evalúe el tipo en 3.9.

## Comandos útiles

```bash
# Entorno (bootstrap del repo)
python setup.py && source .venv/bin/activate

# Datos + modelos
python download_datasets.py
python models/train.py

# Solo validador (consola Rich)
python scripts/validate_edge.py --hours 72

# API + dashboard (supervisor)
AUTO_START=false PORT=8080 python scripts/api.py
# http://127.0.0.1:8080/  —  http://127.0.0.1:8080/health
```

## Convenciones al editar

- Cambios **acotados** al objetivo; no refactors masivos ni archivos de doc no pedidos.
- Mantener coherencia con `models/train.py` en nombres de features y rutas de PKL (`BTCUSDT`, no `BTC` solo).
- Calibración sklearn: `**predict`** en IsotonicRegression (no `transform`).
- Nunca sombrear el módulo `signal` de stdlib con una variable llamada `signal` en el mismo scope.

## Logs en Railway

- Los JSON de **flujos TCP** (netflow) en la consola **no** son logs de la app: no indican si Gamma/Binance fallan.
- Mira **Deploy Logs** del servicio: ahí debe salir Uvicorn y, tras el cambio de supervisor, **`validate_edge`** (prefijo `[validate_edge]`, líneas tipo `Gamma: N mercados…`, `Iteración OK`, warnings de Binance).
- Si **`csv_rows`** en `GET /api/status` (o el pie del dashboard) **sube** cada ~30s con el worker en marcha, se están escribiendo observaciones (Gamma + filtros + precio); si se queda en **0** durante mucho tiempo, o no hay mercados que pasen el filtro o el worker no está arrancado (`AUTO_START` / botón START).

## Límites conocidos

- Gamma devuelve muchos mercados; la paginación del validador está acotada (`GAMMA_MAX_PAGES`) — si no aparecen mercados 5m crypto, subir el límite o revisar filtros de pregunta.
- En vivo `vol_zscore` de volumen no está disponible desde ticker simple; se fija según la lógica documentada en el validador.

