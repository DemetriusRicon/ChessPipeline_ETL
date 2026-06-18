"""
HTTP Client base para o Chess ETL Pipeline.
Implementa:
  - HTTP/2 via httpx
  - Compressão gzip automática
  - Cache condicional (ETag / Last-Modified → 304)
  - Retry com backoff exponencial via tenacity
  - Logging de headers CDN do Chess.com (HIT/MISS/EXPIRED/REVALIDATED)
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)

# ─── Constantes ───────────────────────────────────────────────────────────────
DEFAULT_USER_AGENT = os.getenv(
    "CHESS_COM_USER_AGENT",
    "AntiGravity-ExportTool/1.0 (username: AntiGravityEng; contact: dev@antigravity.com)",
)
RATE_LIMIT_BACKOFF_SECONDS = 60
CDN_STATUS_HEADER = "CF-Cache-Status"  # Chess.com usa Cloudflare


# ─── Exceções customizadas ────────────────────────────────────────────────────
class RateLimitError(Exception):
    """Levantado quando a API retorna HTTP 429."""
    pass


class ResourceGoneError(Exception):
    """Levantado quando a API retorna HTTP 410 (conta encerrada, etc.)."""
    pass


# ─── ETag Cache Store (in-memory simples) ─────────────────────────────────────
_etag_cache: dict[str, dict[str, str]] = {}


def _get_cache_headers(url: str) -> dict[str, str]:
    """Retorna headers de cache condicional para a URL, se disponíveis."""
    cached = _etag_cache.get(url, {})
    headers = {}
    if "etag" in cached:
        headers["If-None-Match"] = cached["etag"]
    if "last_modified" in cached:
        headers["If-Modified-Since"] = cached["last_modified"]
    return headers


def _store_cache_headers(url: str, response: httpx.Response) -> None:
    """Armazena ETag e Last-Modified da resposta para uso futuro."""
    entry: dict[str, str] = {}
    if etag := response.headers.get("ETag"):
        entry["etag"] = etag
    if last_mod := response.headers.get("Last-Modified"):
        entry["last_modified"] = last_mod
    if entry:
        _etag_cache[url] = entry


def _log_cdn_status(url: str, response: httpx.Response) -> None:
    """Loga o status do cache CDN do Chess.com."""
    cdn_status = response.headers.get(CDN_STATUS_HEADER, "UNKNOWN")
    logger.debug("CDN [%s] → %s", cdn_status, url)


# ─── Cliente principal ────────────────────────────────────────────────────────
class ChessHttpClient:
    """
    Cliente HTTP compartilhado para Chess.com e Lichess.
    Usa HTTP/2 e gzip por padrão.
    """

    def __init__(
        self,
        base_url: str = "",
        extra_headers: dict[str, str] | None = None,
        timeout: float = 30.0,
    ) -> None:
        headers = {
            "Accept-Encoding": "gzip",
            "User-Agent": DEFAULT_USER_AGENT,
        }
        if extra_headers:
            headers.update(extra_headers)

        self._client = httpx.Client(
            base_url=base_url,
            headers=headers,
            http2=True,
            timeout=timeout,
            follow_redirects=True,  # Lida com 301 automaticamente
        )

    # ── Método GET com retry e cache condicional ────────────────────────────
    @retry(
        retry=retry_if_exception_type(httpx.TransportError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True,
    )
    def get(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        use_cache: bool = True,
    ) -> httpx.Response | None:
        """
        Realiza um GET com suporte a:
          - Cache condicional (ETag / Last-Modified)
          - Retry automático em erros de transporte
          - Tratamento de 304, 410 e 429

        Retorna:
          - httpx.Response com status 200
          - None se o recurso não foi modificado (304 — usar cache local)
        """
        conditional_headers = _get_cache_headers(url) if use_cache else {}

        logger.info("GET → %s", url)
        response = self._client.get(url, params=params, headers=conditional_headers)

        _log_cdn_status(url, response)

        if response.status_code == 304:
            logger.info("304 Not Modified — usando cache local para %s", url)
            return None

        if response.status_code == 429:
            logger.warning("429 Rate Limit — aguardando %ds...", RATE_LIMIT_BACKOFF_SECONDS)
            time.sleep(RATE_LIMIT_BACKOFF_SECONDS)
            raise RateLimitError(f"Rate limit atingido para {url}")

        if response.status_code == 410:
            logger.error("410 Gone — recurso permanentemente removido: %s", url)
            raise ResourceGoneError(f"Recurso removido: {url}")

        response.raise_for_status()

        if use_cache:
            _store_cache_headers(url, response)

        return response

    # ── Streaming (Lichess ND-JSON) ─────────────────────────────────────────
    def stream_get(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        extra_headers: dict[str, str] | None = None,
    ):
        """
        Abre um stream HTTP e retorna um context manager.
        Use com `with client.stream_get(...) as r: for line in r.iter_lines()`.
        Não faz buffer total em memória — leitura por chunks.
        """
        headers = extra_headers or {}
        logger.info("STREAM GET → %s", url)
        return self._client.stream("GET", url, params=params, headers=headers)

    def post(
        self,
        url: str,
        json: Any | None = None,
        data: Any | None = None,
        extra_headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        """POST simples com retry."""
        headers = extra_headers or {}
        logger.info("POST → %s", url)
        response = self._client.post(url, json=json, data=data, headers=headers)
        response.raise_for_status()
        return response

    def close(self) -> None:
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
