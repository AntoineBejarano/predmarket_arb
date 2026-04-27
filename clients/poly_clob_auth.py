"""L2 HMAC auth para CLOB Polymarket (sin py_clob_client)."""

from __future__ import annotations

import base64
import hashlib
import hmac
import time
from typing import Optional

POLY_ADDRESS = "POLY_ADDRESS"
POLY_SIGNATURE = "POLY_SIGNATURE"
POLY_TIMESTAMP = "POLY_TIMESTAMP"
POLY_API_KEY = "POLY_API_KEY"
POLY_PASSPHRASE = "POLY_PASSPHRASE"


def build_hmac_signature(
    secret_b64: str,
    timestamp: int,
    method: str,
    request_path: str,
    body: Optional[str] = None,
) -> str:
    """
    Firma HMAC-SHA256 como el cliente oficial (secret en base64 url-safe).
    Si body no es None, se concatena con comillas dobles (mismo criterio que TS/Go).
    """
    base64_secret = base64.urlsafe_b64decode(secret_b64)
    message = str(timestamp) + str(method) + str(request_path)
    # Mismo criterio que py_clob_client.signing.hmac: solo concatena si body es truthy
    if body:
        message += str(body).replace("'", '"')
    sig = hmac.new(base64_secret, message.encode("utf-8"), hashlib.sha256).digest()
    return base64.urlsafe_b64encode(sig).decode("utf-8")


def build_l2_headers(
    signer_address: str,
    api_key: str,
    api_secret_b64: str,
    passphrase: str,
    method: str,
    request_path: str,
    body_json: Optional[str],
    timestamp: Optional[int] = None,
) -> dict[str, str]:
    ts = int(timestamp) if timestamp is not None else int(time.time())
    sig = build_hmac_signature(api_secret_b64, ts, method.upper(), request_path, body_json)
    return {
        POLY_ADDRESS: signer_address,
        POLY_SIGNATURE: sig,
        POLY_TIMESTAMP: str(ts),
        POLY_API_KEY: api_key,
        POLY_PASSPHRASE: passphrase,
    }
