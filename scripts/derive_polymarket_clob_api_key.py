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

Por defecto imprime en **stdout** los valores etiquetados y debajo el JSON
indentado (para verlo en terminal o redirigir solo el bloque JSON).

    python scripts/derive_polymarket_clob_api_key.py --json-only > data/polymarket_account.json

No subas ``data/polymarket_account.json`` a git.
"""

from __future__ import annotations

import argparse
import json
import os
import sys


def main() -> int:
    ap = argparse.ArgumentParser(description="Deriva credenciales L2 CLOB Polymarket")
    ap.add_argument(
        "--json-only",
        action="store_true",
        help="Solo imprime JSON (sin bloque etiquetado); útil para redirección a archivo",
    )
    args = ap.parse_args()

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
    if args.json_only:
        print(json.dumps(out, indent=2), flush=True)
        return 0

    print("=== Credenciales L2 (CLOB Polymarket) ===\n", flush=True)
    print(f"api_key:        {out['api_key']}", flush=True)
    print(f"api_secret:     {out['api_secret']}", flush=True)
    print(f"api_passphrase: {out['api_passphrase']}", flush=True)
    print(f"private_key:    {out['private_key']}", flush=True)
    print("\n=== JSON (copiar a data/polymarket_account.json) ===\n", flush=True)
    print(json.dumps(out, indent=2), flush=True)
    print(
        "\n# Copia el bloque JSON a data/polymarket_account.json (o $DATA_DIR/polymarket_account.json).",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
