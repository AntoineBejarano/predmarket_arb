"""Circuit breaker por drawdown diario (paper / capital simulado)."""

from __future__ import annotations


class CircuitBreaker:
    """Para si drawdown supera umbral (capital simulado hasta integrar PnL real)."""

    def __init__(self, max_daily_drawdown: float = 0.08, data_dir: str = "logs") -> None:
        self.max_daily_drawdown = max_daily_drawdown
        self.data_dir = data_dir
        self._tripped = False

    async def check(self, current_capital: float, start_capital: float) -> bool:
        """True si puede operar; False si debe parar."""
        if self._tripped:
            return False
        if start_capital <= 0:
            return True
        drawdown = (start_capital - current_capital) / start_capital
        if drawdown > self.max_daily_drawdown:
            self._tripped = True
            return False
        return True

    def reset(self) -> None:
        """Llamar al inicio de cada día UTC."""
        self._tripped = False
