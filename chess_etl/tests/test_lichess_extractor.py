"""
Testes unitários — Lichess Extractor
Valida o token mock, normalização e DataFrame de saída.
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from extractors.lichess_extractor import (
    LichessExtractor,
    MOCK_TOKEN,
    _normalize_game,
    _resolve_lichess_token,
)


# ─── Testes do Token Resolver ─────────────────────────────────────────────────

class TestTokenResolver:
    def test_returns_mock_when_no_env(self):
        with patch.dict("os.environ", {"LICHESS_API_TOKEN": ""}, clear=False):
            with patch("extractors.lichess_extractor.Variable", side_effect=ImportError):
                token = _resolve_lichess_token()
        assert token == MOCK_TOKEN

    def test_returns_env_token_when_set(self):
        with patch.dict("os.environ", {"LICHESS_API_TOKEN": "real_token_123"}, clear=False):
            with patch("extractors.lichess_extractor.Variable", side_effect=ImportError):
                token = _resolve_lichess_token()
        assert token == "real_token_123"

    def test_airflow_variable_takes_priority(self):
        mock_variable = MagicMock()
        mock_variable.get.return_value = "airflow_token_abc"
        with patch("extractors.lichess_extractor.Variable", mock_variable):
            with patch.dict("os.environ", {"LICHESS_API_TOKEN": "env_token"}, clear=False):
                token = _resolve_lichess_token()
        assert token == "airflow_token_abc"


# ─── Testes do Normalizador ───────────────────────────────────────────────────

class TestNormalizeLichessGame:
    SAMPLE_RAW = {
        "id": "abcd1234",
        "rated": True,
        "variant": "standard",
        "speed": "blitz",
        "perf": "blitz",
        "createdAt": 1710000000000,
        "lastMoveAt": 1710003600000,
        "status": "mate",
        "winner": "white",
        "players": {
            "white": {"user": {"id": "demetrius01", "name": "Demetrius01"}, "rating": 1500},
            "black": {"user": {"id": "opponent", "name": "Opponent"}, "rating": 1480},
        },
        "moves": "e4 e5 Nf3",
        "clock": {"initial": 180, "increment": 2},
    }

    def test_normalize_basic(self):
        result = _normalize_game(self.SAMPLE_RAW)
        assert result["game_id"] == "abcd1234"
        assert result["variant"] == "standard"
        assert result["speed"] == "blitz"
        assert result["status"] == "mate"
        assert result["white_id"] == "demetrius01"
        assert result["white_rating"] == 1500
        assert result["white_result"] == "win"
        assert result["black_id"] == "opponent"
        assert result["black_result"] == "loss"
        assert result["clock_initial"] == 180
        assert result["clock_increment"] == 2
        assert result["moves"] == "e4 e5 Nf3"

    def test_draw_result(self):
        raw = {**self.SAMPLE_RAW, "winner": None, "status": "draw"}
        result = _normalize_game(raw)
        assert result["white_result"] == "draw"
        assert result["black_result"] == "draw"

    def test_timestamps_converted(self):
        result = _normalize_game(self.SAMPLE_RAW)
        assert isinstance(result["created_at"], datetime)
        assert result["created_at"].tzinfo is not None


# ─── Testes do Extractor (modo mock) ─────────────────────────────────────────

class TestLichessExtractorMockMode:
    def test_is_mock_when_no_token(self):
        with patch("extractors.lichess_extractor._resolve_lichess_token", return_value=MOCK_TOKEN):
            extractor = LichessExtractor(username="Demetrius01")
        assert extractor.is_using_mock_token is True

    def test_mock_dataframe_has_expected_columns(self):
        with patch("extractors.lichess_extractor._resolve_lichess_token", return_value=MOCK_TOKEN):
            extractor = LichessExtractor(username="Demetrius01")
        df = extractor._mock_dataframe()

        expected_cols = {
            "game_id", "rated", "variant", "speed", "perf",
            "created_at", "last_move_at", "status",
            "white_id", "white_rating", "white_result",
            "black_id", "black_rating", "black_result",
            "moves", "clock_initial", "clock_increment", "ingestion_ts",
        }
        assert expected_cols.issubset(set(df.columns))

    def test_extract_games_returns_mock_df(self):
        with patch("extractors.lichess_extractor._resolve_lichess_token", return_value=MOCK_TOKEN):
            extractor = LichessExtractor(username="Demetrius01")
        df = extractor.extract_games()

        assert not df.empty
        assert len(df) == 2
        assert df["game_id"].iloc[0] == "mock_game_001"

    def test_mock_game_usernames(self):
        with patch("extractors.lichess_extractor._resolve_lichess_token", return_value=MOCK_TOKEN):
            extractor = LichessExtractor(username="Demetrius01")
        df = extractor.extract_games()

        assert "demetrius01" in df["white_id"].values or "demetrius01" in df["black_id"].values
