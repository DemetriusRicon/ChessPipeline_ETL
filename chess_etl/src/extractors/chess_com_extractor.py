"""
Extractor para a Chess.com PubAPI.

Fluxo de extração:
  1. get_archives(username) → lista de URLs mensais
  2. Para cada URL mensal → extract_monthly_games(url)
  3. Normaliza cada partida para o schema Bronze

Conformidade:
  - User-Agent obrigatório (AntiGravity-ExportTool/1.0)
  - HTTP/2 + gzip
  - ETag / Last-Modified → cache condicional
  - Logging de headers CDN (HIT/MISS/EXPIRED/REVALIDATED)
  - Sem paralelismo (serial por IP)
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Generator

import pandas as pd

from utils.http_client import ChessHttpClient, RateLimitError
from utils.rate_limiter import chess_com_limiter

logger = logging.getLogger(__name__)

CHESS_COM_BASE = "https://api.chess.com"


def _normalize_game(raw: dict, source_month: str) -> dict:
    """
    Normaliza um objeto de partida da Chess.com para o schema Bronze.
    """
    white = raw.get("white", {})
    black = raw.get("black", {})

    return {
        "game_id": raw.get("url", "").split("/")[-1],
        "url": raw.get("url"),
        "pgn": raw.get("pgn"),
        "time_control": raw.get("time_control"),
        "end_time": datetime.fromtimestamp(raw["end_time"], tz=timezone.utc)
        if raw.get("end_time")
        else None,
        "rated": raw.get("rated", False),
        "time_class": raw.get("time_class"),
        "rules": raw.get("rules", "chess"),
        "white_username": white.get("username"),
        "white_rating": white.get("rating"),
        "white_result": white.get("result"),
        "black_username": black.get("username"),
        "black_rating": black.get("rating"),
        "black_result": black.get("result"),
        "ingestion_ts": datetime.now(tz=timezone.utc),
        "source_month": source_month,
    }


class ChessComExtractor:
    """
    Extrator de partidas da Chess.com PubAPI.

    Exemplo de uso:
        extractor = ChessComExtractor(username="demetriusricon")
        for month_url in extractor.get_archives():
            df = extractor.extract_monthly_games(month_url)
            # salvar df em Parquet
    """

    def __init__(self, username: str = "demetriusricon") -> None:
        self.username = username.lower()
        self._client = ChessHttpClient(base_url=CHESS_COM_BASE)

    # ── Perfil ────────────────────────────────────────────────────────────────
    def get_player_profile(self) -> dict:
        """Retorna o perfil público do jogador."""
        with chess_com_limiter.acquire():
            response = self._client.get(f"/pub/player/{self.username}")
        if response is None:
            logger.info("Perfil do jogador '%s' não modificado (304).", self.username)
            return {}
        return response.json()

    # ── Estatísticas ──────────────────────────────────────────────────────────
    def get_player_stats(self) -> dict:
        """Retorna as estatísticas de rating do jogador."""
        with chess_com_limiter.acquire():
            response = self._client.get(f"/pub/player/{self.username}/stats")
        if response is None:
            return {}
        return response.json()

    # ── Índice de Arquivos Mensais ────────────────────────────────────────────
    def get_archives(self) -> list[str]:
        """
        Retorna a lista de URLs de arquivos mensais do jogador.
        Ex: ['https://api.chess.com/pub/player/demetriusricon/games/2025/01', ...]
        """
        with chess_com_limiter.acquire():
            response = self._client.get(
                f"/pub/player/{self.username}/games/archives"
            )
        if response is None:
            logger.info("Archives não modificados (304) para '%s'.", self.username)
            return []

        archives: list[str] = response.json().get("archives", [])
        logger.info(
            "Encontrados %d arquivos mensais para '%s'.",
            len(archives),
            self.username,
        )
        return archives

    # ── Extração Mensal ───────────────────────────────────────────────────────
    def extract_monthly_games(
        self, archive_url: str
    ) -> pd.DataFrame:
        """
        Extrai todas as partidas de um arquivo mensal e retorna um DataFrame.

        Args:
            archive_url: URL completa do arquivo mensal
                         Ex: https://api.chess.com/pub/player/demetriusricon/games/2025/01
        """
        # Extrai YYYY-MM da URL para o campo source_month
        parts = archive_url.rstrip("/").split("/")
        source_month = f"{parts[-2]}-{parts[-1]}"

        logger.info("Extraindo games de %s ...", archive_url)

        with chess_com_limiter.acquire():
            response = self._client.get(archive_url, use_cache=True)

        if response is None:
            logger.info("Arquivo %s não modificado (304) — usando cache.", archive_url)
            return pd.DataFrame()

        games_raw: list[dict] = response.json().get("games", [])
        logger.info(
            "  → %d partidas encontradas em %s", len(games_raw), source_month
        )

        if not games_raw:
            return pd.DataFrame()

        records = [_normalize_game(g, source_month) for g in games_raw]
        df = pd.DataFrame(records)

        # Cast de tipos explícito para consistência
        df["rated"] = df["rated"].astype(bool)
        df["white_rating"] = pd.to_numeric(df["white_rating"], errors="coerce").astype(
            "Int64"
        )
        df["black_rating"] = pd.to_numeric(df["black_rating"], errors="coerce").astype(
            "Int64"
        )

        return df

    # ── Extração Completa (generator por mês) ────────────────────────────────
    def extract_all_games(
        self, last_n_months: int = 0
    ) -> Generator[tuple[str, pd.DataFrame], None, None]:
        """
        Itera pelos arquivos mensais e produz (source_month, DataFrame) por mês.

        Args:
            last_n_months: Se > 0, extrai apenas os últimos N meses.
                           Se 0, extrai todo o histórico.
        """
        archives = self.get_archives()
        if not archives:
            return

        if last_n_months > 0:
            archives = archives[-last_n_months:]

        for archive_url in archives:
            try:
                df = self.extract_monthly_games(archive_url)
                parts = archive_url.rstrip("/").split("/")
                source_month = f"{parts[-2]}-{parts[-1]}"
                if not df.empty:
                    yield source_month, df
            except RateLimitError:
                logger.error("Rate limit atingido — encerrando extração.")
                break
            except Exception as exc:
                logger.error("Erro ao extrair %s: %s", archive_url, exc)
                continue

    def close(self) -> None:
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
