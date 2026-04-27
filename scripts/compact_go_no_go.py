#!/usr/bin/env python3
"""Emite decisión GO/NO-GO a partir de reportes compactos."""

from __future__ import annotations

import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
REPORTS_DIR = REPO_ROOT / "reports"


def main() -> None:
    p = REPORTS_DIR / "compact_eval_report.json"
    if not p.is_file():
        raise SystemExit(f"Falta {p}. Ejecuta scripts/evaluate_compact_vs_baseline.py primero.")
    rep = json.loads(p.read_text(encoding="utf-8"))
    rows = rep.get("assets", [])
    valid = [r for r in rows if r.get("delta_vs_baseline") is not None]
    wins = sum(1 for r in valid if float(r.get("delta_vs_baseline", 0.0)) > 0.0)
    mean_delta = sum(float(r.get("delta_vs_baseline", 0.0)) for r in valid) / len(valid) if valid else 0.0
    decision = "NO-GO"
    reason = "sin mejora consistente frente a baseline"
    if valid and wins >= max(1, int(0.6 * len(valid))) and mean_delta >= 0.003:
        decision = "GO"
        reason = "mejora consistente en mayoría de activos"
    out = {
        "decision": decision,
        "reason": reason,
        "valid_assets": len(valid),
        "wins": wins,
        "mean_delta_vs_baseline": mean_delta,
    }
    out_path = REPORTS_DIR / "compact_go_no_go.json"
    out_path.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
