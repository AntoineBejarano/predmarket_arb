# NegRisk Maker Bundle — RUNBOOK (`bundle_arb`)

Nombre visible: **NegRisk Maker Bundle**. Slug interno: **`bundle_arb`**. Modos: **`BUNDLE_MODE=taker_scan`** (legacy, cruza asks) y **`maker_first`** (cotiza bids maker con misma `q`).

## Principio matemático (NegRisk estándar)

En eventos **negative risk** de Polymarket, **solo un outcome entre los hijos puede ganar** (mutuamente excluyentes). La documentación distingue NegRisk estándar (conjunto de outcomes conocido al crear el mercado) de **NegRisk augmented** (pueden añadirse outcomes / “Other” dinámico). Este bot, por defecto, **solo considera NegRisk estándar** (`negRiskAugmented` excluido salvo `BUNDLE_ALLOW_AUGMENTED_NEGRISK=true`).

Cada **child market** binario tiene tokens Yes/No. La estrategia compra el **token YES** de cada pierna. Si la suma de precios de compra ejecutables de todos los YES es **menor que 1** (menos fees y buffer de ejecución), hay edge teórico de cesta.

Referencias: [Markets & Events](https://docs.polymarket.com/concepts/markets-events), [Negative Risk](https://docs.polymarket.com/advanced/neg-risk), [Fetching Markets](https://docs.polymarket.com/market-data/fetching-markets), [List events keyset](https://docs.polymarket.com/api-reference/events/list-events-keyset-pagination).

## Qué es un candidato (regla operativa)

Un **candidato es un Gamma `event`**, no un slug ni un título fijos:

- `event.active == true` y `event.closed == false`
- `event.negRisk == true`
- Por defecto: `event.negRiskAugmented != true`
- `BUNDLE_MIN_DAYS_TO_EXPIRY <= días_hasta(event.endDate) <= BUNDLE_MAX_DAYS_TO_EXPIRY` (UTC)
- Entre `BUNDLE_MIN_OUTCOMES` y `BUNDLE_MAX_OUTCOMES` **child markets** que pasen filtros de tradability y tengan token **YES** identificable
- Revalidación CLOB opcional: `GET /markets/{conditionId}` por pierna
- Precios: `best_ask` del libro o **VWAP** hasta `BUNDLE_TARGET_SIZE_USDC` por pierna si `BUNDLE_USE_VWAP=true`
- Edge: `1 - sum(precio_efectivo_i) - execution_buffer - fees_est > BUNDLE_MIN_EDGE`

## Descubrimiento (`BUNDLE_DISCOVERY`)

| Modo | Origen | Uso |
|------|--------|-----|
| `gamma_events` (**default**) | `GET …/events/keyset?active=true&closed=false` + `next_cursor` | Eventos NegRisk con `markets[]` anidados; token YES por hijo. |
| `gamma` | `GET …/markets` offset/limit | Lista plana de mercados (legacy). |
| `clob_simplified` | CLOB `/simplified-markets` | Compacto CLOB. |
| `clob_full` | CLOB `/markets` paginado | Legacy; muchas filas inactivas al inicio del cursor. |

Keyset: respuesta `{ "events": [...], "next_cursor": "..." }`.

## Filtros hijo (Gamma + CLOB)

Por cada `market` en `event.markets`:

- `active`, `!closed`, `!archived`, `!restricted` (Gamma)
- `enableOrderBook`, `acceptingOrders` (camelCase o snake_case)
- `clobTokenIds` + `outcomes` parseables; índice donde `outcomes[i].lower() == "yes"` alinea con `clobTokenIds[i]`

## Parámetros (env)

| Variable | Default (orientativo) | Descripción |
|----------|----------------------|-------------|
| `BUNDLE_DISCOVERY` | `gamma_events` | Modo de descubrimiento. |
| `BUNDLE_REQUIRE_NEGRISK` | `true` | En `gamma_events`, exige `event.negRisk`. |
| `BUNDLE_ALLOW_AUGMENTED_NEGRISK` | `false` | Si `true`, permite `negRiskAugmented`. |
| `BUNDLE_MIN_DAYS_TO_EXPIRY` | `14` | Rechaza eventos que cierran antes. |
| `BUNDLE_MAX_DAYS_TO_EXPIRY` | `365` | Evita lockup extremo. |
| `BUNDLE_GAMMA_EVENTS_MAX_PAGES` | `20` | Páginas keyset máx. por ciclo. |
| `BUNDLE_GAMMA_EVENTS_LIMIT` | `50` | `limit` por request keyset. |
| `BUNDLE_MAX_CANDIDATES_PER_CYCLE` | `120` | Tope tras scoring. |
| `BUNDLE_MIN_EDGE` | `0.02` | Umbral neto (sube si hay ruido). |
| `BUNDLE_MAX_SIZE_USDC` | `300` | Notional objetivo del bundle. |
| `BUNDLE_TARGET_SIZE_USDC` | `50` | Notional por pierna para VWAP (y profundidad). |
| `BUNDLE_USE_VWAP` | `false` | VWAP sobre asks hasta notional/pierna. |
| `BUNDLE_MIN_DEPTH_PER_LEG_USDC` | `0` | Mínimo USDC ejecutable al mejor nivel (opcional). |
| `BUNDLE_EXEC_BUFFER_PER_LEG` | `0.0025` | Buffer por pierna (sustituye en parte el gas fijo). |
| `BUNDLE_GAS_PER_LEG` | `0.012` | Heurística legacy si buffer muy bajo. |
| `BUNDLE_EXCLUDE_NEG_RISK` | `true` | Solo aplica a modos **no** `gamma_events` (ahí los YES son NegRisk a propósito). |

## Scoring (v1)

Orden previo a martillar CLOB: combinación de `log1p(liquidityClob)`, `log1p(volume24hr)`, ventana de días al cierre y `1/n` outcomes. Pesos simplificados en código (`BUNDLE_SCORE_*` opcionales en el futuro).

## Heurísticas que **no** resuelve solo `negRisk`

| Tipo | Apto automático |
|------|-----------------|
| Winner / elección / award (un ganador) | Sí con NegRisk + filtros |
| Top-N, varios Yes, umbrales anidados | No sin reglas extra (fase 2) |
| Deportes / timing especial | Revisión manual o filtros adicionales |

## CSV / señal

- `market_id`: prefijo `negRisk_event:{event_id}` (trazabilidad).
- `n_outcomes`: número de piernas YES.
- `sum_ask`: suma de precios efectivos (best o VWAP).

## Riesgos

- Resolución y reglas del evento: revisar documentación del mercado antes de `DRY_RUN=false`.
- Augmented NegRisk: conjunto dinámico; excluido por defecto.
- VWAP: requiere profundidad real; sin liquidez, skip sin señal.

---

## Modo `maker_first` (plan v3)

### Inventario

**Hasta que todas las piernas estén llenas con la misma cantidad de shares `q`, hay riesgo de inventario.** No es arbitraje libre de riesgo antes de un bundle balanceado.

### postOnly y cruce de libro

- `BUNDLE_POST_ONLY=true` en maker: órdenes **postOnly**.
- Si el CLOB devuelve algo equivalente a **`invalid post-only order: order crosses book`**: **no** reintentar como marketable; **refrescar libro** y **recalcular el bundle completo** (quoter).

### Sizing: misma `q` (shares)

- Objetivo de cartera: **`BUNDLE_TARGET_BUNDLE_USDC`** (notional aproximado del bundle).
- `q = target_bundle_usdc / sum(bid_i)` tras escala; en cada pierna `notional_i = q * bid_i`.
- No usar “mismo USDC por pierna” como unidad principal.

### `ExecutionPolicy` (2a + 2b)

- **2a:** dataclass estable con default **`unknown`** conservador: sin `taker_entry`, edges altos (`arb/negrisk_execution_policy.py`).
- **2b:** `fee_free` solo si: ningún hijo tiene `feesEnabled` **true** (Gamma), existe señal fiable de fee-rate CLOB, y **todos** los `fee_rate_bps` muestreados son **0**. Si `any_child.feesEnabled is True` → **nunca** `fee_free`. Categoría/tags solo contexto, no prueba de fee-free.

### Batch y atomicidad

- El endpoint batch (hasta **15** órdenes) **no** es una transacción ACID: la API puede procesar en **paralelo** y devolver **HTTP 200** con errores **por orden** en el cuerpo.
- **Atomicidad por código:** inspeccionar cada resultado; si alguna orden falla → **cancelar** las ya aceptadas del evento (best-effort) y marcar **HALTED** / revisión.

### Reconciliación (tres fuentes)

1. `getOpenOrders` (por `asset_id` / mercado).
2. `getOrder(id)` — `size_matched`, `original_size`, etc.
3. `getTrades` — fills por `asset_id` / mercado / cursor temporal.

Sin persistencia cargable + reconcile + camino de **cancel** → **no live** maker.

### Kill switch (Fase 5.5)

- Tras **N** fallos consecutivos de reconcile (`BUNDLE_RECONCILE_FAIL_MAX_CYCLES`): cancelar órdenes conocidas y **desactivar posting live** hasta intervención.
- Si `state == PARTIAL` y reconcile falla: **no** nuevas órdenes; intentar cancel; estado **`NEEDS_MANUAL_REVIEW`**.

### Salidas parciales

- **`normal_exit`:** venta **maker postOnly**.
- **`emergency_exit`:** venta **taker** solo si policy lo permite y timeout / pérdida máxima — última defensa si el maker no liquida.

### Artefactos JSON (separados del scan taker)

| Archivo | Uso |
|---------|-----|
| `logs/bundle_arb_scan.json` | Diagnóstico UI modo **taker_scan** (y campos compartidos). |
| `logs/negrisk_maker_state.json` | FSM persistido por `event_id`. |
| `logs/negrisk_maker_events.json` | Diagnóstico por ciclo maker (quotes, policy, reconcile). |

### Variables maker (añadir a `.env`)

| Variable | Default | Descripción |
|----------|---------|-------------|
| `BUNDLE_MODE` | `taker_scan` | `maker_first` activa pipeline maker. |
| `BUNDLE_TARGET_BUNDLE_USDC` | `50` | Objetivo notional bundle para derivar `q`. |
| `BUNDLE_MAX_OUTCOMES_LIVE` | `4` | Tope de piernas para **live** (paper puede usar `BUNDLE_MAX_OUTCOMES`). |
| `BUNDLE_MAKER_LIVE` | `false` | `true` permite POST (requiere credenciales; gates reconcile). |
| `BUNDLE_POST_ONLY` | `true` | Maker real. |
| `BUNDLE_ORDER_TYPE` | `GTD` | `GTC` o `GTD` (+ `BUNDLE_ORDER_TTL_SECONDS`, mín. ~60s margen doc). |
| `BUNDLE_RECONCILE_FAIL_MAX_CYCLES` | `3` | Kill switch global. |
| `BUNDLE_MIN_LIQUIDITY_CLOB` | `0` | Filtro liquidez evento Gamma (0=off). |

### Enlaces útiles

- [Fees](https://docs.polymarket.com/trading/fees)
- [Order lifecycle](https://docs.polymarket.com/concepts/order-lifecycle)
- [Maker rebates](https://docs.polymarket.com/market-makers/maker-rebates)
