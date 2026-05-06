#!/usr/bin/env python3
"""
Auditoría offline de latency_arb_sports.csv: inconsistencias estructurales,
spam de SIGNAL, edges extremos, coherencia home/away.

Uso:
  python scripts/audit_latency_sports_csv.py --csv /ruta/a/latency_arb_sports.csv
  python scripts/audit_latency_sports_csv.py --csv ... --snapshots /ruta/a/latency_arb_sports_snapshots.csv
  python scripts/audit_latency_sports_csv.py --csv ... --json-out report.json
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from arb.latency_sports_signal_sanity import (  # noqa: E402
    MAX_ABS_EDGE_FOR_SIGNAL,
    MAX_SUM_EDGE_MAG,
    normalize_team_name,
    normalized_team_pair,
    validate_market_row,
)


def _parse_ts(s: str) -> Optional[datetime]:
    s = (s or "").strip()
    if not s:
        return None
    try:
        t = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if t.tzinfo is None:
            t = t.replace(tzinfo=timezone.utc)
        return t
    except ValueError:
        return None


def _f(x: Any) -> Optional[float]:
    if x is None or x == "":
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def load_rows(path: Path) -> list[dict[str, str]]:
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def audit_signals(rows: list[dict[str, str]]) -> dict[str, Any]:
    signals = [r for r in rows if (r.get("action") or "").strip().upper() == "SIGNAL"]
    invalid: list[dict[str, Any]] = []
    reasons_count: dict[str, int] = defaultdict(int)

    # --- |edge| > 0.15
    for r in signals:
        ee = _f(r.get("edge_exec"))
        em = _f(r.get("edge_mid"))
        eg = _f(r.get("edge"))
        edge_u = ee if ee is not None else (em if em is not None else eg)
        if edge_u is not None and abs(edge_u) > MAX_ABS_EDGE_FOR_SIGNAL:
            invalid.append({"row": r, "reason": "EDGE_IMPLAUSIBLE", "detail": f"|edge|={abs(edge_u):.4f}"})
            reasons_count["EDGE_IMPLAUSIBLE"] += 1

    # --- validate market
    seen_ml: set[int] = set()
    for i, r in enumerate(signals):
        ok, why = validate_market_row(r)
        if not ok:
            invalid.append({"row": r, "reason": "BAD_MARKET", "detail": why})
            reasons_count["BAD_MARKET"] += 1
            seen_ml.add(i)

    # --- duplicate_signal_spam: same canonical pair + side, multiple SIGNAL < 5s
    by_pair_side: dict[tuple[tuple[str, str], str], list[tuple[int, datetime, dict]]] = defaultdict(list)
    for i, r in enumerate(signals):
        ts = _parse_ts(r.get("ts") or "")
        if ts is None:
            continue
        h, a = r.get("home_team") or "", r.get("away_team") or ""
        pair = normalized_team_pair(h, a)
        side = (r.get("side") or "").strip()
        by_pair_side[(pair, side)].append((i, ts, r))

    spam_keys: set[tuple[tuple[str, str], str]] = set()
    for key, lst in by_pair_side.items():
        lst.sort(key=lambda x: x[1])
        for j in range(1, len(lst)):
            dt = (lst[j][1] - lst[j - 1][1]).total_seconds()
            if 0 <= dt < 5.0:
                spam_keys.add(key)
                break
    for key in spam_keys:
        reasons_count["DUPLICATE_SIGNAL_SPAM"] += 1
        for _, _, r in by_pair_side[key]:
            invalid.append({"row": r, "reason": "DUPLICATE_SIGNAL_SPAM", "detail": f"key={key}"})

    # --- both sides positive SIGNAL in short window (2s) same pair
    pair_window: dict[tuple[str, str], list[tuple[datetime, str, float, dict]]] = defaultdict(list)
    for r in signals:
        ts = _parse_ts(r.get("ts") or "")
        if ts is None:
            continue
        pair = normalized_team_pair(r.get("home_team") or "", r.get("away_team") or "")
        edge = _f(r.get("edge")) or _f(r.get("edge_exec")) or 0.0
        pair_window[pair].append((ts, (r.get("side") or "").strip(), float(edge), r))

    for pair, lst in pair_window.items():
        lst.sort(key=lambda x: x[0])
        for j in range(len(lst)):
            t0, s0, e0, r0 = lst[j]
            for k in range(j + 1, len(lst)):
                t1, s1, e1, r1 = lst[k]
                if (t1 - t0).total_seconds() > 2.0:
                    break
                if s0 != s1 and e0 > 0 and e1 > 0:
                    invalid.append(
                        {
                            "row": r1,
                            "reason": "BOTH_SIDES_POSITIVE_WINDOW",
                            "detail": f"pair={pair}",
                        }
                    )
                    reasons_count["BOTH_SIDES_POSITIVE_WINDOW"] += 1

    # Dedupe invalid list by (ts, token_id, side, reason) weakly
    def row_key(x: dict[str, Any]) -> tuple:
        rr = x["row"]
        return (rr.get("ts"), rr.get("token_id"), rr.get("side"), x["reason"])

    uniq: dict[tuple, dict[str, Any]] = {}
    for inv in invalid:
        uniq[row_key(inv)] = inv
    invalid = list(uniq.values())

    n_sig = len(signals)
    pct_fake = (len(invalid) / max(n_sig, 1)) * 100.0 if n_sig else 0.0

    # Top 5 suspicious by |edge| among signals that appear in invalid OR |edge|>0.15
    scored: list[tuple[float, dict[str, str]]] = []
    for r in signals:
        eg = _f(r.get("edge_exec")) or _f(r.get("edge")) or 0.0
        scored.append((abs(eg), r))
    scored.sort(key=lambda x: -x[0])
    top5 = scored[:5]

    return {
        "total_rows": len(rows),
        "signal_rows": n_sig,
        "invalid_signal_hits": len(invalid),
        "pct_signals_flagged": round(pct_fake, 2),
        "reasons_count": dict(reasons_count),
        "invalid": [
            {
                "reason": x["reason"],
                "detail": x.get("detail"),
                "ts": x["row"].get("ts"),
                "home_team": x["row"].get("home_team"),
                "away_team": x["row"].get("away_team"),
                "side": x["row"].get("side"),
                "edge": x["row"].get("edge"),
                "game_slug": x["row"].get("game_slug"),
                "odds_io_event_id": x["row"].get("odds_io_event_id"),
            }
            for x in invalid[:500]
        ],
        "top5_abs_edge": [
            {
                "ts": r.get("ts"),
                "home": r.get("home_team"),
                "away": r.get("away_team"),
                "side": r.get("side"),
                "abs_edge": abs(_f(r.get("edge_exec")) or _f(r.get("edge")) or 0.0),
            }
            for _, r in top5
        ],
    }


def audit_snapshots(snap_path: Path) -> dict[str, Any]:
    if not snap_path.is_file():
        return {"available": False}
    rows = load_rows(snap_path)
    bad: list[dict[str, Any]] = []
    for r in rows:
        dh = _f(r.get("delta_home"))
        da = _f(r.get("delta_away"))
        ph = _f(r.get("betfair_prob_home"))
        pa = _f(r.get("betfair_prob_away"))
        if dh is not None and da is not None and abs(dh + da) > MAX_SUM_EDGE_MAG:
            bad.append(
                {
                    "ts": r.get("ts"),
                    "game": r.get("game"),
                    "delta_home": dh,
                    "delta_away": da,
                    "sum": dh + da,
                }
            )
        if ph is not None and pa is not None:
            s = ph + pa
            if abs(s - 1.0) > 1e-3:
                bad.append({"ts": r.get("ts"), "game": r.get("game"), "prob_sum": s, "reason": "prob_sum"})
    return {"available": True, "inconsistent_pricing_rows": len(bad), "examples": bad[:20]}


def main() -> None:
    ap = argparse.ArgumentParser(description="Auditoría latency_arb_sports CSV")
    ap.add_argument("--csv", type=Path, required=True, help="Ruta a latency_arb_sports.csv")
    ap.add_argument("--snapshots", type=Path, default=None, help="Opcional: latency_arb_sports_snapshots.csv")
    ap.add_argument("--json-out", type=Path, default=None)
    args = ap.parse_args()
    if not args.csv.is_file():
        print(f"ERROR: no existe {args.csv}", file=sys.stderr)
        sys.exit(1)
    rows = load_rows(args.csv)
    out: dict[str, Any] = {"csv": str(args.csv.resolve()), "signals_audit": audit_signals(rows)}
    if args.snapshots:
        out["snapshots_audit"] = audit_snapshots(args.snapshots)
    text = json.dumps(out, indent=2, ensure_ascii=False)
    print(text)
    if args.json_out:
        args.json_out.write_text(text, encoding="utf-8")


if __name__ == "__main__":
    main()
