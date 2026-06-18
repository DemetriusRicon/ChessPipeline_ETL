"""
Rate limiter serial para Chess ETL Pipeline.
Garante que apenas uma requisição por vez seja feita a cada provedor,
conforme exigência das APIs (especialmente Lichess).
"""

from __future__ import annotations

import logging
import threading
import time
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class SerialRateLimiter:
    """
    Limiter serial que garante acesso exclusivo a um provedor de API.
    Thread-safe via Lock.

    Uso:
        limiter = SerialRateLimiter(min_interval=1.0)
        with limiter.acquire():
            response = client.get(url)
    """

    def __init__(self, min_interval: float = 1.0, name: str = "default") -> None:
        """
        Args:
            min_interval: Intervalo mínimo em segundos entre requisições.
            name: Nome do limiter para logging.
        """
        self._lock = threading.Lock()
        self._last_request_ts: float = 0.0
        self._min_interval = min_interval
        self._name = name

    @contextmanager
    def acquire(self):
        """Context manager que bloqueia até poder fazer a próxima requisição."""
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_request_ts
            wait_time = self._min_interval - elapsed

            if wait_time > 0:
                logger.debug(
                    "[RateLimiter:%s] Aguardando %.2fs antes da próxima requisição",
                    self._name,
                    wait_time,
                )
                time.sleep(wait_time)

            self._last_request_ts = time.monotonic()
            try:
                yield
            finally:
                pass  # Lock liberado automaticamente pelo with

    def backoff(self, seconds: float = 60.0) -> None:
        """
        Força uma pausa completa (para uso após receber 429).
        Bloqueia outras threads durante a espera.
        """
        with self._lock:
            logger.warning(
                "[RateLimiter:%s] Backoff de %.0fs iniciado (HTTP 429)",
                self._name,
                seconds,
            )
            time.sleep(seconds)
            self._last_request_ts = time.monotonic()


# ─── Instâncias globais por provedor ─────────────────────────────────────────
# Chess.com: sem limite explícito, mas serial e respeito ao TTL de 12h
chess_com_limiter = SerialRateLimiter(min_interval=0.5, name="chess_com")

# Lichess: 1 req/vez, política serial estrita
lichess_limiter = SerialRateLimiter(min_interval=1.0, name="lichess")
