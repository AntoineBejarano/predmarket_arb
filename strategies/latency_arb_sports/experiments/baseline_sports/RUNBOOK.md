# RUNBOOK — baseline_sports (`latency_arb_sports`)

## Objetivo

Paper / laboratorio: comparar probabilidad fair Pinnacle (The Odds API) con mid CLOB Polymarket en mercados deportivos Gamma, con matching explícito y TTL de descubrimiento anti-ban.

## Variables de entorno

- **`ENABLE_EXPERIMENTAL`**: por defecto en `arb_engine.py` es **activo** (`true`) para no depender de variables en Railway. Solo si defines `ENABLE_EXPERIMENTAL=false` el motor queda en 3 estrategias base y **no** cargará `latency_arb_sports` aunque el toggle en la UI esté ON.
- Con el motor en marcha, en Deploy Logs verás líneas `[latency_arb_sports] cycle #…` con conteos de **public-search** (HTTP vs aciertos de caché TTL), Odds API y matches (solo si esta estrategia está cargada).
- `DRY_RUN=true` (recomendado) — no envía órdenes; `false` + credenciales L2 para POST `/order` FOK.
- `LATENCY_SPORTS_MIN_EDGE`, `LATENCY_SPORTS_SPORTS`, `LATENCY_SPORTS_POLL_INTERVAL`, `LATENCY_SPORTS_MAX_STAKE_USDC`, `LATENCY_SPORTS_REGIONS` — ver `.env.example`.
- `LATENCY_SPORTS_DISCOVERY_TTL=300` — default y **mínimo** 300 s (clamp en runtime); no bajar para no martillar Gamma. El resultado de **public-search por partido** se cachea con este TTL (no se repite el GET cada ciclo de poll para el mismo evento Odds).
- `ODDS_API_KEY` — preferido sobre clave LAB en código.
- `LATENCY_SPORTS_GAMMA_PUBLIC_SEARCH_LIMIT` — límite de eventos pedidos a `GET /public-search` (default 40).

## Gamma: tags y discovery

En Gamma, los **tags no son “deporte genérico”** (no hay un tag estable equivalente a `soccer_epl` de Odds). Los `tag_id` / `tag_slug` suelen ir **por competición concreta** (IDs o slugs tipo liga o torneo). Por eso esta estrategia **no** depende de mapear `sport_key` → tag Gamma.

**Discovery actual:** por cada partido Odds (Pinnacle h2h), se llama a `GET {GAMMA}/public-search?q={home}+{away}` y se filtran eventos cuyo `title`/`slug` contengan **ambos** equipos vía `teams_match_odds_gamma` (misma heurística Levenshtein que el match fino en mercados).

## Reglas de match (Gamma ↔ Odds API)

1. **Deporte:** el `sport_key` solo etiqueta el bucket Odds (EPL, NBA, …); el enlace a Gamma es por **texto de equipos + ventana temporal**, no por inferencia de deporte en mercados Gamma.
2. Equipos: `teams_match_odds_gamma` en home y away frente al blob `title`+`slug` del evento Gamma (y en outcomes del mercado hijo al construir la fila CLOB).
3. Tiempo: `abs(commence_gamma - commence_odds) < 3600` s UTC, donde `commence_gamma` es el **más cercano** entre texto `scheduled for`, `endDate` y `startDate` del mercado Gamma elegido dentro del evento.

## Arranque

```bash
source .venv/bin/activate
export ENABLE_EXPERIMENTAL=true
export DRY_RUN=true
python scripts/arb_engine.py
```

O desde el API: Motor Arb → Start; activar toggle `latency_arb_sports` en `/arb` o en `/arb/strategy/latency_arb_sports`.

## CSV

Ruta: `$DATA_DIR/logs/latency_arb_sports.csv` (columnas `ts`, `action`, `reason`, … + `edge`, `status`).

## UI

- Índice: `/arb`
- Detalle: `/arb/strategy/latency_arb_sports` (`static/latency_sports.html`)

## Interpretación de señales

- `action=SIGNAL` / `EXECUTED`: oportunidad con `edge` positivo (fair − mid) por encima de `LATENCY_SPORTS_MIN_EDGE`.
- `SKIP:LOW_EDGE` / `SKIP:CIRCUIT_BREAKER`: sin trade o breaker activo.

## Conocimiento: Betfair vs Polymarket (mercados sharp)

**Betfair es generalmente el mercado más “sharp” en comparación con Polymarket, especialmente para un sistema de value betting.**  
Esto se debe a su alta liquidez, participación de apostadores profesionales y eficiencia en precios que refleja información precisa rápidamente. [caanberry](https://caanberry.com/why-is-betfair-exchange-pricing-so-accurate/)

### ¿Qué significa “mercado sharp”?

Un mercado sharp tiene odds eficientes, con bajo margen (vig), alta liquidez y movimiento rápido ante nueva información, minimizando ineficiencias explotables. [punter2pro](https://punter2pro.com/best-sharp-sportsbooks-betting-sites/)  
Betfair Exchange destaca por su modelo peer-to-peer, donde pros y bots ajustan precios cerca de bookies sharp como Pinnacle. [caanberry](https://caanberry.com/why-is-betfair-exchange-pricing-so-accurate/)  
Polymarket, enfocado en predicciones (política, cripto), muestra más ineficiencias por bots y arbitraje. [dlnews](https://www.dlnews.com/articles/markets/polymarket-users-lost-millions-of-dollars-to-bot-like-bettors-over-the-past-year/)

### Ventajas de Betfair como benchmark

- **Liquidez masiva en deportes**: miles de millones en volumen anual, spreads ajustados (1.5–3% margen), superior en fútbol, tenis y caballos. [betaminic](https://www.betaminic.com/es/estudios-de-apuestas/que-ligas-de-futbol-tienen-mas-liquidez-en-betfair/)
- **Precios precisos**: correlacionados con bookies sharp; útil como referencia (“oráculo”) para detectar edge en otros sitios. [reddit](https://www.reddit.com/r/algobetting/comments/1p72c9i/using_polymarket_as_an_oracle_to_find_ev_bets/)
- **Comisiones**: 2–5% en ganancias netas; penaliza winners con “Expert Fee” (hasta 40% extra). [startpolymarket](https://startpolymarket.com/reviews/best-prediction-markets/)

### Ineficiencias en Polymarket

- **Arbitraje frecuente**: bots ganaron ~$40M en un año explotando mispricings, especialmente en política (sumas por debajo del 100% en outcomes). [finance.yahoo](https://finance.yahoo.com/news/polymarket-silent-gold-rush-sharp-092527911.html)
- **Menos sharp en general**: retail-heavy, delays en fills y spreads amplios fuera de majors; odds divergen de fuentes sharp como CME. [edgescouts](https://www.edgescouts.com/blog/polymarket-vs-sportsbooks-where-edges-hide)
- **Fees bajos (0–2%)**, pero las ineficiencias crean edge —p. ej. discrepancia tipo 84.6% Betfair vs 91% Poly, donde Betfair suele estar más alineado con pros. [newspoly](https://www.newspoly.net/blog/polymarket-vs-sports-betting)

### Justificación para el sistema (fair externo vs mid Poly)

Un setup de value bets con **Betfair (u otra fuente sharp) como base** puede tener edge real porque Betfair es más sharp en eventos con liquidez compartida (deportes / predicciones híbridas). [reddit](https://www.reddit.com/r/algobetting/comments/1p72c9i/using_polymarket_as_an_oracle_to_find_ev_bets/)  
Riesgo si Poly captura información única (cripto/política), pero en muchos casos Betfair converge más rápido a la “verdad” de mercado. [startpolymarket](https://startpolymarket.com/reviews/best-prediction-markets/)  
En una ventana corta de discrepancia, un edge implícito del orden **91/84.6 − 1 ≈ 6.3%** puede ser viable incluso sin depender solo de latency arb puro. [edgescouts](https://www.edgescouts.com/blog/polymarket-vs-sportsbooks-where-edges-hide)

> **Nota de alineación con este repo:** el experimento baseline usa **Pinnacle vía The Odds API** como fair reference en código; la lógica de “mercado sharp” y benchmark deportivo es análoga a la de Betfair descrita aquí.

| Aspecto | Betfair | Polymarket |
|--------|---------|------------|
| Liquidez principal | Deportes (profunda) | Política/cripto (alta en majors) [startpolymarket](https://startpolymarket.com/reviews/best-prediction-markets/) |
| Eficiencia odds | Muy alta (sigue Pinnacle) | Media (arbs, bots) [caanberry](https://caanberry.com/why-is-betfair-exchange-pricing-so-accurate/) |
| Edge para value | Benchmark ideal | Oportunidades, pero más riesgo [reddit](https://www.reddit.com/r/algobetting/comments/1p72c9i/using_polymarket_as_an_oracle_to_find_ev_bets/) |
| Fees | ~5% + Expert | menor del 2% en profits [newspoly](https://www.newspoly.net/blog/polymarket-vs-sports-betting) |
