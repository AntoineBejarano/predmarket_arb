# PredMarket Arb — guía para asistentes (Claude / Cursor)

## Qué es este repo

Sistema de **paper trading / validación de edge** sobre mercados Polymarket tipo “5m crypto Up or Down”. Compara probabilidad del mercado (Gamma API) con un modelo ML (LightGBM + isotónica) y una heurística NearRes. **No hay trades ni wallet** en el validador clásico: solo lectura de APIs y logs en CSV.

Además hay un **Arb Engine** opcional (`scripts/arb_engine.py`): laboratorio de estrategias de arbitraje / market making sobre CLOB Polymarket (y stubs Kalshi) con CSV bajo `DATA_DIR`. Por defecto **`DRY_RUN=true`**: no envía órdenes reales; con `DRY_RUN=false` y credenciales L2, `clients/poly_clob.py` puede **POST /order** al CLOB usando `py-order-utils` + `eth-account` (sin el SDK `py_clob_client`).

El repo está organizado como **laboratorio de estrategias** (`strategies/`): cada estrategia tiene `strategy.yaml`, experimentos bajo `experiments/<slug>/` con `RUNBOOK.md` + `LEARNINGS.md` y métricas pequeñas en git. Ver [strategies/README.md](strategies/README.md) (inglés). Las carpetas `strategies/bundle_arb`, `cross_exchange`, `market_maker`, etc. documentan bots del arb engine.

## Arquitectura en procesos


| Proceso                         | Rol                                                                                                                                                                  |
| ------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `**scripts/api.py`**            | FastAPI: **validador ML** — **catálogo ML** `/ml` (`ml_models.html`), **monitor** por slug `/ml/model/<slug>` y **`/`** (mismo HTML que `crypto_5m_lgbm`, `ml_model_detail.html`); JSON `/api/status`, `/api/ml/models`, `POST /api/ml/models/<slug>/enable|disable`, START/STOP del worker, SSE `/api/signals/live`, `/health` → `{"status":"ok"}` para Railway. **Motor Arb (CLOB)** — `POST/GET /api/arb/start|stop|status`, toggles por estrategia, log CSV, SSE `/api/arb/signals/live`. |
| `**scripts/validate_edge.py`**  | Worker largo: Binance + Gamma, Rich en consola, escribe `logs/signals.csv`. Arrancado por el API como subproceso o en local a mano.                                  |
| `**scripts/arb_engine.py`**     | Subproceso opcional lanzado desde el API: bucle de estrategias en `arb/` + `risk/`; usa `clients/poly_clob.py` (REST CLOB, órdenes si `DRY_RUN=false`).                |


- Comunicación validador: `**logs/signals.csv`** (ruta bajo `DATA_DIR`) y PID en memoria del API.
- **Catálogo de modelos ML (UI):** definiciones en `risk/ml_model_registry.py` (slug, label, `implemented`, etc.); toggles `enabled` en `**data/model_state.json**` vía `risk/model_state.py`. **`validate_edge` aún no elige pipeline por slug**: un solo worker escribe el mismo CSV; los toggles preparan multi-modelo futuro.
- Comunicación arb: CSV por estrategia bajo `DATA_DIR` / `logs` (según rutas en `scripts/api.py` y `data/`).
- **Puertos:** el API usa `PORT` (p. ej. 8080). El worker tiene su propio mini HTTP de health en `**VALIDATOR_HEALTH_PORT`** (default `18088`) para no colisionar con el API.

## Archivos importantes

- `models/train.py` — entrena y guarda PKL en `models/saved/{BTCUSDT,...}_*.pkl`. Las features en vivo del validador están alineadas con este pipeline (velas ~5m sintéticas desde ticks de 30s).
- `scripts/validate_edge.py` — bucle principal, resolución de ventanas, informes diarios UTC, `SIGNAL_THRESHOLD`, etc.
- `scripts/arb_engine.py` — punto de entrada del arb engine; estrategias en `arb/`, riesgo en `risk/`, clientes HTTP en `clients/`.
- `risk/ml_model_registry.py` — slugs y metadatos del catálogo ML servidos por `/api/ml/models`. `risk/model_state.py` — lectura/escritura de `enabled` por slug (async lock, mismo patrón que `risk/strategy_state.py`).
- `clients/poly_clob.py` — CLOB REST (`/markets`, `/book`, `/order`, cancelaciones L2), WebSocket `subscribe_market` (payload tipo `market` con `assets_ids`). Firma de órdenes: `clients/poly_order_live.py` + `clients/poly_clob_auth.py` (HMAC L2 alineado con `py_clob_client`).
- `clients/kalshi_rest.py` — cliente REST Kalshi (stubs donde aplique).
- `scripts/healthcheck.py` — handler HTTP mínimo usado por el worker.
- `static/ml_models.html` — **catálogo ML** (`/ml`). `static/ml_model_detail.html` — **monitor validador** (métricas, tablas, Gamma debug, START/STOP) en **`/`** y **`/ml/model/<slug>`**; slugs no implementados muestran vista “en desarrollo”. `static/arb.html` — **motor Arb (CLOB)** (`/arb`). `static/arb_strategy_detail.html` — detalle por estrategia (`/arb/strategy/<slug>`). Navegación cruzada con nombres: Catálogo ML · Monitor validador · Motor Arb (CLOB).
- `Dockerfile` / `railway.toml` — arranque con `python scripts/api.py`; healthcheck Railway en `/health` del API.

En `**python:3.11-slim`** hace falta el paquete `**libgomp1`** (OpenMP) o LightGBM falla al cargar PKL: `libgomp.so.1: cannot open shared object file` — ya instalado en el `Dockerfile`.

Los `**.pkl`** (calibradores `IsotonicRegression`) deben cargarse con la **misma familia de `scikit-learn`** que al entrenar; en `pyproject.toml` está acotado a **1.6.x** para evitar `InconsistentVersionWarning` y resultados raros si Docker instala 1.8+.

Órdenes CLOB en vivo requieren **`eth-account`** y **`py-order-utils`**. El runtime oficial del repo es **Python ≥ 3.11** (`pyproject.toml`, `setup.py` y imagen Docker); no uses 3.9.x para este proyecto.

## Entorno y variables

- Copiar `.env.example` → `.env`. Relevantes: `DATA_DIR`, `PORT`, `AUTO_START`, `SIGNAL_THRESHOLD`, `LOG_LEVEL`, `GAMMA_MAX_PAGES`, `VALIDATOR_HEALTH_PORT`, `DASHBOARD_PASSWORD` (reservado; auth no implementada).
- **Arb / CLOB:** `DRY_RUN` (default seguro `true`), `POLY_API_KEY`, `POLY_API_SECRET`, `POLY_PASSPHRASE`, `POLY_PRIVATE_KEY`; opcionales `POLY_FUNDER`, `POLY_SIGNATURE_TYPE`, `POLYGON_CHAIN_ID`. Sin secret L2, `place_order` en vivo falla con mensaje explícito.
- Python del proyecto: **≥ 3.11** obligatorio (`python setup.py` sale con error si no). Docker `python:3.11-slim` cumple. En rutas FastAPI evitar anotaciones `X | Y` sin `from __future__ import annotations` donde haga falta compatibilidad con intérpretes viejos.
- **Arb en Railway:** con `DRY_RUN=true` y estrategias **desactivadas** por defecto en `data/strategy_state.json`, el despliegue levanta la UI lista; el motor no llama al CLOB hasta que actives toggles y pulses Start.
- **Catálogo ML en UI:** `data/model_state.json` guarda `enabled` por slug (alineado con `risk/ml_model_registry.py`). Sin efecto sobre qué código corre en `validate_edge` hasta que se cablee lectura de ese estado o variables de entorno.
- **Capital ficticio (paper):** por estrategia en `data/strategy_state.json` (`fict_capital_eur`, default `ARB_FICT_CAPITAL_EUR=1000`). Cada `SIGNAL`/`EXECUTED` con `edge` y `size_usdc` acumula `fict_pnl_est ≈ stake×edge` y ROI en `/api/arb/status` y CSV (`fict_*` columnas).

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
# http://127.0.0.1:8080/ (monitor validador) · http://127.0.0.1:8080/ml (catálogo ML) · http://127.0.0.1:8080/ml/model/crypto_5m_lgbm (monitor por slug)
# http://127.0.0.1:8080/arb (motor Arb CLOB) · http://127.0.0.1:8080/arb/strategy/bundle_arb (detalle estrategia) — http://127.0.0.1:8080/health
```

## Convenciones al editar

- Cambios **acotados** al objetivo; no refactors masivos ni archivos de doc no pedidos.
- Mantener coherencia con `models/train.py` en nombres de features y rutas de PKL (`BTCUSDT`, no `BTC` solo).
- Calibración sklearn: `**predict`** en IsotonicRegression (no `transform`).
- Nunca sombrear el módulo `signal` de stdlib con una variable llamada `signal` en el mismo scope.

## Logs en Railway

- Los JSON de **flujos TCP** (netflow) en la consola **no** son logs de la app: no indican si Gamma/Binance fallan.
- Mira **Deploy Logs** del servicio: ahí debe salir Uvicorn y, tras el cambio de supervisor, `**validate_edge`** (prefijo `[validate_edge]`, líneas tipo `Gamma: N mercados…`, `Iteración OK`, warnings de Binance).
- El API escribe con prefijo `**[api]`**: arranque (`DATA_DIR`, `AUTO_START`), `POST /api/start` / `stop`, y apagado.
- Cada iteración del validador (~30s) loguea `**Iteración #N`**: mercados Gamma tras filtro, observaciones totales, señales, resueltos, pendientes, **nº de velas 5m por activo** y **último precio spot** por activo (visibilidad Binance).
- Si `**csv_rows`** en `GET /api/status` (o el pie del panel ML en detalle) **sube** cada ~30s con el worker en marcha, se están escribiendo observaciones (Gamma + filtros + precio); si se queda en **0** durante mucho tiempo, o no hay mercados que pasen el filtro o el worker no está arrancado (`AUTO_START` / botón START).

## Límites conocidos

- Gamma devuelve muchos mercados; la paginación del validador está acotada (`GAMMA_MAX_PAGES`) — si no aparecen mercados 5m crypto, subir el límite o revisar filtros de pregunta.
- En vivo `vol_zscore` de volumen no está disponible desde ticker simple; se fija según la lógica documentada en el validador.
- El WebSocket `subscribe_market` sigue el formato documentado del canal `market`; si Polymarket cambia el mensaje de suscripción o la URL, ajustar `clients/poly_clob.py`.
- Kalshi u otros conectores pueden seguir con métodos no implementados fuera de los flujos ya cableados.
- Añadir un modelo nuevo en la UI: entrada en `risk/ml_model_registry.py` (y clave en `data/model_state.json` si quieres estado versionado en git); implementar pipeline en `validate_edge.py` y, si aplica, CSV/API distintos antes de marcar `implemented: true` en el registro.

