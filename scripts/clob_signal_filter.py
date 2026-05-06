"""Compara probabilidad del modelo ML vs precio YES del CLOB de Polymarket
para detectar mispricing. Usado en tiempo real por el pipeline de ejecución."""

from __future__ import annotations


class CLOBSignalFilter:
    def evaluate(
        self,
        model_prob: float,
        clob_yes_price: float,
        min_edge: float = 0.05,
        min_liquidity_usdc: float = 50.0,
        liquidity: float = 0.0,
    ) -> dict:
        """
        Retorna dict con:
        - signal: bool — True si hay edge suficiente
        - edge: float — ventaja a favor del lado operado (>= 0 si hay ventaja neta)
        - direction: "YES" | "NO"
        - reason: str — explicación de la decisión

        Lógica:
        - UP (model_prob > 0.5): edge = model_prob - clob_yes_price
        - DOWN (model_prob <= 0.5): prob_down_model = 1 - model_prob,
          prob_down_clob = 1 - clob_yes_price (implícita en precio YES),
          edge = prob_down_model - prob_down_clob (= clob_yes_price - model_prob).
        - Si edge < 0 no hay ventaja en esa dirección → signal = False.
        - signal = True solo si edge >= min_edge AND liquidity >= min_liquidity_usdc.
        """
        if model_prob > 0.5:
            direction = "YES"
            edge = model_prob - clob_yes_price
        else:
            direction = "NO"
            prob_down_model = 1.0 - model_prob
            prob_down_clob = 1.0 - clob_yes_price
            edge = prob_down_model - prob_down_clob

        if liquidity < min_liquidity_usdc:
            return {
                "signal": False,
                "edge": float(edge),
                "direction": direction,
                "reason": (
                    f"liquidez insuficiente ({liquidity:.2f} < {min_liquidity_usdc} USDC)"
                ),
            }

        if edge < 0:
            return {
                "signal": False,
                "edge": float(edge),
                "direction": direction,
                "reason": f"sin ventaja ({direction}: edge={edge:.4f} < 0)",
            }

        if edge < min_edge:
            return {
                "signal": False,
                "edge": float(edge),
                "direction": direction,
                "reason": f"edge insuficiente ({edge:.4f} < {min_edge})",
            }

        return {
            "signal": True,
            "edge": float(edge),
            "direction": direction,
            "reason": f"edge {edge:.4f} >= {min_edge} y liquidez OK",
        }


if __name__ == "__main__":
    f = CLOBSignalFilter()

    r = f.evaluate(0.65, 0.50, min_edge=0.05, min_liquidity_usdc=50.0, liquidity=100.0)
    assert r["signal"] is True
    assert r["direction"] == "YES"
    assert abs(r["edge"] - 0.15) < 1e-9

    r2 = f.evaluate(0.35, 0.50, min_edge=0.05, min_liquidity_usdc=50.0, liquidity=100.0)
    assert r2["signal"] is True
    assert r2["direction"] == "NO"
    assert abs(r2["edge"] - 0.15) < 1e-9

    r3 = f.evaluate(0.65, 0.62, min_edge=0.05, min_liquidity_usdc=50.0, liquidity=100.0)
    assert r3["signal"] is False
    assert r3["edge"] < 0.05

    r4 = f.evaluate(0.65, 0.50, min_edge=0.05, min_liquidity_usdc=50.0, liquidity=10.0)
    assert r4["signal"] is False
    assert "liquidez" in r4["reason"].lower()

    r5 = f.evaluate(0.5, 0.40, min_edge=0.05, min_liquidity_usdc=50.0, liquidity=100.0)
    assert r5["direction"] == "NO"
    assert abs(r5["edge"] - (-0.1)) < 1e-9
    assert r5["signal"] is False
    assert "ventaja" in r5["reason"].lower() or "edge" in r5["reason"].lower()

    r6 = f.evaluate(0.55, 0.70, min_edge=0.05, min_liquidity_usdc=50.0, liquidity=100.0)
    assert r6["direction"] == "YES"
    assert r6["edge"] < 0
    assert r6["signal"] is False

    print("clob_signal_filter: todos los tests inline OK")
