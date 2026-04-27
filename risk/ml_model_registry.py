"""Catálogo de modelos del validador ML (UI + API). validate_edge sigue un solo pipeline hasta ampliar."""

from __future__ import annotations

from typing import Any

# Slugs estables para rutas /ml/model/{slug} y data/model_state.json
ML_MODELS: list[dict[str, Any]] = [
    {
        "slug": "crypto_5m_lgbm",
        "label": "Crypto 5m — LightGBM + NearRes",
        "description": "Polymarket Gamma (5m up/down) vs modelo por activo y heurística NearRes; escribe logs/signals.csv.",
        "implemented": True,
        "experimental": False,
    },
    {
        "slug": "multi_horizon_stub",
        "label": "Multi-horizon (reservado)",
        "description": "Hueco para un segundo stack de features / horizonte sin tocar el pipeline actual.",
        "implemented": False,
        "experimental": True,
    },
]

ML_MODEL_SLUGS: tuple[str, ...] = tuple(str(m["slug"]) for m in ML_MODELS)


def model_by_slug(slug: str) -> dict[str, Any] | None:
    s = slug.strip()
    for m in ML_MODELS:
        if m["slug"] == s:
            return dict(m)
    return None
