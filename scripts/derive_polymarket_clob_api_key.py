#!/usr/bin/env python3
"""
Crea o deriva credenciales L2 del CLOB Polymarket (apiKey / secret / passphrase).

Documentación: https://docs.polymarket.com/api-reference/authentication

Qué hace esto (versión tonta): tú solo pones la **private key** de la wallet.
El script llama a Polymarket con una firma hecha con esa key; el servidor te
devuelve (o reutiliza) el trio api_key / secret / passphrase **ligado a esa
wallet**. No son tres números que “salen” de tu clave en local como si fuera
un hash: las genera o recupera el CLOB en remoto.

Si en el .env mezclas una private key de la wallet A con un api_key copiado
a mano del panel que en realidad era de la wallet B (o de un par viejo
revocado), Polymarket responde con el error del signer distinto al de la API key.

Cuentas **Magic / email / Google** (Polymarket): la PK exportada es un **EOA**;
L2 y ``POLY_ADDRESS`` van con esa dirección. El **funder** debe ser la **wallet
proxy** que muestra la UI (p. ej. «Address API use only»), con
``POLY_SIGNATURE_TYPE=1`` (POLY_PROXY). No uses ``signature_type=3`` (POLY_1271)
con esa PK salvo flujo deposit relayer explícito: el CLOB puede responder que
el firmante de la orden no coincide con la API key.

Flujo **deposit wallet** puro (``POLY_SIGNATURE_TYPE=3``): relayer + ERC-1271;
este script no sustituye ese onboarding.

Requisito: **private key** (hex con ``0x``, 64 hex de cuerpo). *No* es el UUID
del api_key del dashboard.

Uso (elige una)::

    python scripts/derive_polymarket_clob_api_key.py --private-key '0xabc...'

    export PRIVATE_KEY='0x...'   # o POLY_PRIVATE_KEY
    python scripts/derive_polymarket_clob_api_key.py

Para **Magic/email**, pon ``POLY_SIGNATURE_TYPE=1`` y ``POLY_FUNDER=<proxy UI>``
en el entorno **antes** de derivar. Para **deposit** relayer, ``3`` + funder
deposit (ver docs Polymarket).

Por defecto imprime la **dirección del firmante** (compruébala con la del
perfil Polymarket), luego los cuatro valores y un bloque JSON de referencia.

``scripts/polymarket_client.py`` solo lee ``POLY_API_KEY``, ``POLY_API_SECRET``,
``POLY_PASSPHRASE`` y ``POLY_PRIVATE_KEY`` del entorno; copia ahí los valores
(p. ej. Railway / ``.env``). ``--json-only`` sirve para pegar en un gestor de
secretos o documentación local; no subas credenciales a git.
"""

from __future__ import annotations

import argparse
import json
import os
import sys


def _clob_client_class():
    try:
        from py_clob_client_v2.client import ClobClient

        return ClobClient, "py_clob_client_v2"
    except ImportError:
        try:
            from py_clob_client.client import ClobClient  # type: ignore[no-redef]

            return ClobClient, "py_clob_client"
        except ImportError:
            return None, ""


def main() -> int:
    ap = argparse.ArgumentParser(description="Deriva credenciales L2 CLOB Polymarket")
    ap.add_argument(
        "--private-key",
        "-k",
        default="",
        help="Clave privada hex (0x...). Si no se pasa, usa PRIVATE_KEY o POLY_PRIVATE_KEY del entorno.",
    )
    ap.add_argument(
        "--json-only",
        action="store_true",
        help="Solo imprime JSON (sin bloque etiquetado); referencia / secret manager",
    )
    args = ap.parse_args()

    key = (args.private_key or os.getenv("PRIVATE_KEY") or os.getenv("POLY_PRIVATE_KEY") or "").strip()
    if not key:
        print(
            "Pon la clave de wallet, por ejemplo:\n"
            "  python scripts/derive_polymarket_clob_api_key.py --private-key '0x....'\n"
            "o export PRIVATE_KEY / POLY_PRIVATE_KEY (no es el UUID api_key del panel).",
            file=sys.stderr,
        )
        return 1
    if not key.startswith("0x"):
        key = "0x" + key

    host = os.getenv("POLY_CLOB_HOST", "https://clob.polymarket.com").rstrip("/")
    chain_id = int(os.getenv("POLYGON_CHAIN_ID", "137"))

    signer_address = ""
    try:
        from eth_account import Account

        signer_address = str(Account.from_key(key).address)
    except Exception as e:
        print(f"Private key no válida como clave Ethereum: {e}", file=sys.stderr)
        return 1

    ClobClient, pkg = _clob_client_class()
    if ClobClient is None:
        print("Instala: py-clob-client-v2 o py-clob-client (pyproject.toml)", file=sys.stderr)
        return 1

    # Deposit-wallet / proxy: Polymarket expects L1 derive with the same
    # signature_type + funder as order placement, or POST /order may reject
    # ("order signer address has to be the address of the API KEY").
    funder = os.getenv("POLY_FUNDER", "").strip() or None
    st_raw = os.getenv("POLY_SIGNATURE_TYPE", "").strip()
    if st_raw == "" and funder is not None:
        # Magic/Google: proxy + POLY_PROXY (1). Deposit relayer: set explicit 3.
        signature_type: int | None = 1
    elif st_raw == "":
        signature_type = None
    else:
        signature_type = int(st_raw)

    if signature_type is not None or funder is not None:
        client = ClobClient(host, chain_id, key, None, signature_type, funder)
        print(
            f"[derive] ClobClient(signature_type={signature_type!r}, funder={funder!r}) "
            f"antes de create_or_derive_api_key (lee POLY_* del entorno / .env).",
            file=sys.stderr,
            flush=True,
        )
    else:
        client = ClobClient(host, chain_id, key)
    if hasattr(client, "create_or_derive_api_creds"):
        creds = client.create_or_derive_api_creds()
    else:
        creds = client.create_or_derive_api_key()
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

    print("=== Comprueba esta dirección en Polymarket (debe ser la de tu perfil / API) ===\n", flush=True)
    print(f"signer_address: {signer_address}", flush=True)
    print(f"(cliente CLOB: {pkg})\n", flush=True)
    print("=== Credenciales L2 (CLOB Polymarket) ===\n", flush=True)
    print(f"api_key:        {out['api_key']}", flush=True)
    print(f"api_secret:     {out['api_secret']}", flush=True)
    print(f"api_passphrase: {out['api_passphrase']}", flush=True)
    print(f"private_key:    {out['private_key']}", flush=True)
    print("\n=== JSON (referencia; el runtime usa solo POLY_* en env) ===\n", flush=True)
    print(json.dumps(out, indent=2), flush=True)
    print(
        "\n# Define POLY_API_KEY, POLY_API_SECRET, POLY_PASSPHRASE, POLY_PRIVATE_KEY en .env o Railway.",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
