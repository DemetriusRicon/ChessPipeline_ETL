"""
Testes unitários — Chess.com Extractor
Usa mock HTTP para evitar chamadas reais à API.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Adiciona src/ ao path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from extractors.chess_com_extractor import ChessComExtractor, _normalize_game


# ─── Fixtures ─────────────────────────────────────────────────────────────────

MOCK_PROFILE = {
    "@id": "https://api.chess.com/pub/player/demetriusricon",
    "username": "demetriusricon",
    "player_id": 99999,
    "title": None,
    "status": "basic",
}

MOCK_ARCHIVES = {
    "archives": [
        "https://api.chess.com/pub/player/demetriusricon/games/2025/01",
        "https://api.chess.com/pub/player/demetriusricon/games/2025/02",
    ]
}

MOCK_GAMES = {
    "games": [
        {
            "url": "https://www.chess.com/game/live/111",
            "pgn": "[Event \"Live Chess\"]\n1. e4 e5",
            "time_control": "180+2",
            "end_time": 1736284800,
            "rated": True,
            "time_class": "blitz",
            "rules": "chess",
            "white": {"username": "demetriusricon", "rating": 1200, "result": "win"},
            "black": {"username": "opponent1", "rating": 1180, "result": "loss"},
        },
        {
            "url": "https://www.chess.com/game/live/222",
            "pgn": "[Event \"Live Chess\"]\n1. d4 d5",
            "time_control": "60",
            "end_time": 1736288400,
            "rated": True,
            "time_class": "bullet",
            "rules": "chess",
            "white": {"username": "opponent2", "rating": 1300, "result": "win"},
            "black": {"username": "demetriusricon", "rating": 1205, "result": "loss"},
        },
    ]
}


# ─── Helper: cria response mock ───────────────────────────────────────────────

def _make_response(data: dict, status_code: int = 200):
    response = MagicMock()
    response.status_code = status_code
    response.json.return_value = data
    response.headers = {"CF-Cache-Status": "HIT", "ETag": '"abc123"'}
    return response


# ─── Testes ───────────────────────────────────────────────────────────────────

class TestNormalizeGame:
    def test_normalize_basic_game(self):
        raw = MOCK_GAMES["games"][0]
        result = _normalize_game(raw, "2025-01")

        assert result["game_id"] == "111"
        assert result["white_username"] == "demetriusricon"
        assert result["white_rating"] == 1200
        assert result["white_result"] == "win"
        assert result["black_username"] == "opponent1"
        assert result["rated"] is True
        assert result["time_class"] == "blitz"
        assert result["source_month"] == "2025-01"

    def test_normalize_missing_end_time(self):
        raw = {**MOCK_GAMES["games"][0], "end_time": None}
        result = _normalize_game(raw, "2025-01")
        assert result["end_time"] is None

    def test_game_id_extracted_from_url(self):
        raw = {**MOCK_GAMES["games"][0], "url": "https://www.chess.com/game/live/999"}
        result = _normalize_game(raw, "2025-01")
        assert result["game_id"] == "999"


class TestChessComExtractor:
    @patch("extractors.chess_com_extractor.ChessHttpClient")
    def test_get_archives(self, MockClient):
        mock_client_instance = MockClient.return_value
        mock_client_instance.get.return_value = _make_response(MOCK_ARCHIVES)

        extractor = ChessComExtractor(username="demetriusricon")
        extractor._client = mock_client_instance

        with patch("extractors.chess_com_extractor.chess_com_limiter") as mock_limiter:
            mock_limiter.acquire.return_value.__enter__ = MagicMock(return_value=None)
            mock_limiter.acquire.return_value.__exit__ = MagicMock(return_value=False)
            archives = extractor.get_archives()

        assert len(archives) == 2
        assert "2025/01" in archives[0]
        assert "2025/02" in archives[1]

    @patch("extractors.chess_com_extractor.ChessHttpClient")
    def test_extract_monthly_games_returns_dataframe(self, MockClient):
        mock_client_instance = MockClient.return_value
        mock_client_instance.get.return_value = _make_response(MOCK_GAMES)

        extractor = ChessComExtractor(username="demetriusricon")
        extractor._client = mock_client_instance

        with patch("extractors.chess_com_extractor.chess_com_limiter") as mock_limiter:
            mock_limiter.acquire.return_value.__enter__ = MagicMock(return_value=None)
            mock_limiter.acquire.return_value.__exit__ = MagicMock(return_value=False)
            df = extractor.extract_monthly_games(
                "https://api.chess.com/pub/player/demetriusricon/games/2025/01"
            )

        assert not df.empty
        assert len(df) == 2
        assert "game_id" in df.columns
        assert "white_username" in df.columns
        assert "black_username" in df.columns
        assert df["rated"].dtype == bool

    @patch("extractors.chess_com_extractor.ChessHttpClient")
    def test_extract_returns_empty_on_304(self, MockClient):
        mock_client_instance = MockClient.return_value
        mock_client_instance.get.return_value = None  # Simula 304

        extractor = ChessComExtractor(username="demetriusricon")
        extractor._client = mock_client_instance

        with patch("extractors.chess_com_extractor.chess_com_limiter") as mock_limiter:
            mock_limiter.acquire.return_value.__enter__ = MagicMock(return_value=None)
            mock_limiter.acquire.return_value.__exit__ = MagicMock(return_value=False)
            df = extractor.extract_monthly_games(
                "https://api.chess.com/pub/player/demetriusricon/games/2025/01"
            )

        assert df.empty
