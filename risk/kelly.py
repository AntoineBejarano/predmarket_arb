"""Fracción Kelly con reducción de seguridad."""


def kelly_size(
    edge: float,
    odds: float,
    capital: float,
    fraction: float = 0.5,
    max_pct: float = 0.15,
) -> float:
    """
    edge: probabilidad de ganar según nuestro modelo
    odds: pago en $ por $ apostado (ej: 1.0 para binario 50/50)
    fraction: fracción del Kelly completo (0.5 = half-Kelly)
    max_pct: máximo % del capital total por trade
    """
    if odds <= 0 or edge <= 0:
        return 0.0
    f = (edge * (odds + 1) - 1) / odds
    f_adjusted = f * fraction
    return min(max(f_adjusted * capital, 0.0), capital * max_pct)
