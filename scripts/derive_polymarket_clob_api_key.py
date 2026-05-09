#!/usr/bin/env python3
"""
Crea o deriva credenciales L2 del CLOB Polymarket (apiKey / secret / passphrase).

Documentación: https://docs.polymarket.com/api-reference/authentication

Requisito: **private key de la wallet Ethereum** que usa Polymarket (hex con ``0x``,
64+ caracteres). *No* es el UUID tipo ``apiKey`` que ves en el dashboard.

Este repo usa ``py-clob-client`` (import ``py_clob_client``). Si usas un paquete
``py_clob_client_v2``, la llamada es análoga: ``ClobClient(host, chain_id, key)``
y luego crear/derivar credenciales según esa versión.

Uso::

    export PRIVATE_KEY='0x...'   # o pásala solo en esta sesión
    python scripts/derive_polymarket_clob_api_key.py

Salida: JSON con las claves ``api_key``, ``api_secret``, ``api_passphrase`` y
``private_key`` para pegar en ``data/polymarket_account.json`` (ver
``data/polymarket_account.json.example``). No subas ese archivo a git.
"""

from __future__ import annotations

import json
import os
import sys


def main() -> int:
    key = (os.getenv("PRIVATE_KEY") or os.getenv("POLY_PRIVATE_KEY") or "").strip()
    if not key:
        print(
            "Falta PRIVATE_KEY o POLY_PRIVATE_KEY en el entorno (clave de wallet, no UUID de API).",
            file=sys.stderr,
        )
        return 1
    if not key.startswith("0x"):
        key = "0x" + key

    host = os.getenv("POLY_CLOB_HOST", "https://clob.polymarket.com").rstrip("/")
    chain_id = int(os.getenv("POLYGON_CHAIN_ID", "137"))

    try:
        from py_clob_client.client import ClobClient
    except ImportError:
        print("Instala dependencias: py-clob-client (pyproject / requirements)", file=sys.stderr)
        return 1

    client = ClobClient(host, chain_id, key)
    creds = client.create_or_derive_api_creds()
    if creds is None:
        print("No se pudieron crear/derivar credenciales (revisa la private key y red).", file=sys.stderr)
        return 1

    out = {
        "api_key": creds.api_key,
        "api_secret": creds.api_secret,
        "api_passphrase": creds.api_passphrase,
        "private_key": key,
    }
    print(json.dumps(out, indent=2))
    print(
        "\n# Copia el JSON a data/polymarket_account.json (ruta bajo DATA_DIR si usas otro directorio).",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
