"""
Extractor para a Lichess.org API.

Estratégia de token (em ordem de prioridade):
  1. Airflow Variable 'lichess_api_token'  (produção)
  2. Variável de ambiente LICHESS_API_TOKEN  (desenvolvimento local)
  3. Token mockado MOCK_LICHESS_TOKEN_REPLACE_ME  (testes sem credenciais)

Fluxo de extração:
  - Streaming ND-JSON linha a linha (sem buffer total em memória)
  - Rate limit serial: 1 req/vez, backoff de 60s em 429
  - Suporte a filtros: max, since, until, perfType, color
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Generator

import pandas as pd

from utils.http_client import ChessHttpClient, RateLimitError
from utils.rate_limiter import lichess_limiter

logger = logging.getLogger(__name__)

LICHESS_BASE = "https://lichess.org"

# ─── Token Mock para testes ───────────────────────────────────────────────────
MOCK_TOKEN = "MOCK_LICHESS_TOKEN_REPLACE_ME"


def _resolve_lichess_token() -> str:
    """
    Resolve o token Lichess com fallback em três camadas:
      1. Airflow Variable 'lichess_api_token'
      2. Variável de ambiente LICHESS_API_TOKEN
      3. Token mockado (MOCK_LICHESS_TOKEN_REPLACE_ME)
    """
    # Camada 1 — Airflow Variable (produção)
    try:
        from airflow.models import Variable
        token = Variable.get("lichess_api_token", default_var=None)
        if token and token != MOCK_TOKEN:
            logger.info("Token Lichess carregado via Airflow Variable.")
            return token
    except ImportError:
        pass  # Airflow não disponível fora do container
    except Exception as exc:
        logger.warning("Falha ao ler Airflow Variable: %s", exc)

    # Camada 2 — Variável de ambiente
    env_token = os.getenv("LICHESS_API_TOKEN", "").strip()
    if env_token and env_token != MOCK_TOKEN:
        logger.info("Token Lichess carregado via variável de ambiente.")
        return env_token

    # Camada 3 — Mock (desenvolvimento/testes)
    logger.warning(
        "⚠️  Usando token MOCKADO para Lichess. "
        "Defina LICHESS_API_TOKEN ou configure a Airflow Variable 'lichess_api_token' "
        "para usar a API real."
    )
    return MOCK_TOKEN


def _normalize_game(raw: dict) -> dict:
    """
    Normaliza um objeto de partida Lichess (ND-JSON) para o schema Bronze.
    """
    players = raw.get("players", {})
    white = players.get("white", {})
    black = players.get("black", {})
    clock = raw.get("clock", {})

    # Determina resultado a partir de 'winner'
    winner = raw.get("winner")  # 'white', 'black' ou None (draw)

    def result_for(color: str) -> str:
        if winner is None:
            return "draw"
        return "win" if winner == color else "loss"

    created_at_ms = raw.get("createdAt")
    last_move_at_ms = raw.get("lastMoveAt")

    return {
        "game_id": raw.get("id"),
        "rated": raw.get("rated", False),
        "variant": raw.get("variant", "standard"),
        "speed": raw.get("speed"),
        "perf": raw.get("perf"),
        "created_at": datetime.fromtimestamp(created_at_ms / 1000, tz=timezone.utc)
        if created_at_ms
        else None,
        "last_move_at": datetime.fromtimestamp(last_move_at_ms / 1000, tz=timezone.utc)
        if last_move_at_ms
        else None,
        "status": raw.get("status"),
        "white_id": white.get("user", {}).get("id"),
        "white_rating": white.get("rating"),
        "white_result": result_for("white"),
        "black_id": black.get("user", {}).get("id"),
        "black_rating": black.get("rating"),
        "black_result": result_for("black"),
        "moves": raw.get("moves", ""),
        "clock_initial": clock.get("initial"),
        "clock_increment": clock.get("increment"),
        "ingestion_ts": datetime.now(tz=timezone.utc),
    }


class LichessExtractor:
    """
    Extrator de partidas da Lichess.org via streaming ND-JSON.

    O token é resolvido automaticamente:
      - Em produção (Airflow): via Variable 'lichess_api_token'
      - Em desenvolvimento: via env LICHESS_API_TOKEN
      - Em testes: token mockado MOCK_LICHESS_TOKEN_REPLACE_ME

    Exemplo de uso:
        extractor = LichessExtractor(username="Demetrius01")
        df = extractor.extract_games(max_games=500)
    """

    def __init__(self, username: str = "Demetrius01") -> None:
        self.username = username
        self._token = _resolve_lichess_token()
        self._is_mock = self._token == MOCK_TOKEN

        auth_header = (
            {}
            if self._is_mock
            else {"Authorization": f"Bearer {self._token}"}
        )

        self._client = ChessHttpClient(
            base_url=LICHESS_BASE,
            extra_headers=auth_header,
        )

    @property
    def is_using_mock_token(self) -> bool:
        """True se o extrator está operando com token mockado."""
        return self._is_mock

    # ── Stream de partidas ────────────────────────────────────────────────────
    def _stream_games(
        self,
        params: dict | None = None,
    ) -> Generator[dict, None, None]:
        """
        Lê o stream ND-JSON linha a linha, sem buffer total em memória.
        Cada linha é um objeto JSON representando uma partida.
        """
        url = f"/api/games/user/{self.username}"
        headers = {"Accept": "application/x-ndjson"}

        with lichess_limiter.acquire():
            with self._client.stream_get(url, params=params, extra_headers=headers) as response:
                if response.status_code == 429:
                    logger.warning("429 Rate Limit Lichess — aguardando 60s...")
                    lichess_limiter.backoff(60)
                    raise RateLimitError("Rate limit Lichess")

                response.raise_for_status()

                for line in response.iter_lines():
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        yield json.loads(line)
                    except json.JSONDecodeError as exc:
                        logger.warning("Linha ND-JSON inválida ignorada: %s | %s", line[:100], exc)

    # ── Extração com normalização ─────────────────────────────────────────────
    def extract_games(
        self,
        max_games: int = 0,
        since_ms: int | None = None,
        until_ms: int | None = None,
        perf_type: str | None = None,
        color: str | None = None,
    ) -> pd.DataFrame:
        """
        Extrai partidas do usuário via stream ND-JSON e retorna DataFrame Bronze.

        Args:
            max_games: Limite de partidas (0 = sem limite).
            since_ms: Timestamp inicial em milissegundos.
            until_ms: Timestamp final em milissegundos.
            perf_type: Filtro por categoria (bullet, blitz, rapid, classical).
            color: Filtro por cor ('white' | 'black').
        """
        if self._is_mock:
            logger.warning(
                "⚠️  Token mockado — retornando DataFrame de exemplo vazio. "
                "Configure o token real para extração efetiva."
            )
            return self._mock_dataframe()

        params: dict = {"moves": "true", "clocks": "false", "opening": "false"}
        if max_games > 0:
            params["max"] = max_games
        if since_ms:
            params["since"] = since_ms
        if until_ms:
            params["until"] = until_ms
        if perf_type:
            params["perfType"] = perf_type
        if color:
            params["color"] = color

        records = []
        count = 0
        for raw_game in self._stream_games(params=params):
            records.append(_normalize_game(raw_game))
            count += 1
            if count % 100 == 0:
                logger.info("  → %d partidas processadas (streaming)...", count)

        logger.info("Stream concluído: %d partidas extraídas de '%s'.", count, self.username)

        if not records:
            return pd.DataFrame()

        df = pd.DataFrame(records)
        df["rated"] = df["rated"].astype(bool)
        df["white_rating"] = pd.to_numeric(df["white_rating"], errors="coerce").astype("Int64")
        df["black_rating"] = pd.to_numeric(df["black_rating"], errors="coerce").astype("Int64")
        df["clock_initial"] = pd.to_numeric(df["clock_initial"], errors="coerce").astype("Int64")
        df["clock_increment"] = pd.to_numeric(df["clock_increment"], errors="coerce").astype("Int64")

        return df

    # ── Mock para testes sem token ────────────────────────────────────────────
    def _mock_dataframe(self) -> pd.DataFrame:
        """
        Retorna um DataFrame de exemplo com 2 partidas mockadas.
        Útil para desenvolvimento e CI sem credenciais Lichess.
        """
        mock_records = [
            {
                "game_id": "mock_game_001",
                "rated": True,
                "variant": "standard",
                "speed": "blitz",
                "perf": "blitz",
                "created_at": datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
                "last_move_at": datetime(2025, 1, 15, 12, 5, 0, tzinfo=timezone.utc),
                "status": "mate",
                "white_id": "demetrius01",
                "white_rating": 1500,
                "white_result": "win",
                "black_id": "opponent_mock",
                "black_rating": 1480,
                "black_result": "loss",
                "moves": "e4 e5 Nf3 Nc6 Bb5 a6 Ba4 Nf6",
                "clock_initial": 180,
                "clock_increment": 2,
                "ingestion_ts": datetime.now(tz=timezone.utc),
            },
            {
                "game_id": "mock_game_002",
                "rated": True,
                "variant": "standard",
                "speed": "bullet",
                "perf": "bullet",
                "created_at": datetime(2025, 1, 16, 9, 0, 0, tzinfo=timezone.utc),
                "last_move_at": datetime(2025, 1, 16, 9, 2, 0, tzinfo=timezone.utc),
                "status": "resign",
                "white_id": "opponent_mock_2",
                "white_rating": 1600,
                "white_result": "win",
                "black_id": "demetrius01",
                "black_rating": 1505,
                "black_result": "loss",
                "moves": "d4 d5 c4 e6 Nc3 Nf6 Bg5",
                "clock_initial": 60,
                "clock_increment": 0,
                "ingestion_ts": datetime.now(tz=timezone.utc),
            },
        ]
        logger.info("Retornando %d partidas MOCKADAS.", len(mock_records))
        return pd.DataFrame(mock_records)

    def close(self) -> None:
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
