"""
src/loaders/__init__.py
"""
from .parquet_loader import save_chess_com_games, save_lichess_games, read_parquet

__all__ = ["save_chess_com_games", "save_lichess_games", "read_parquet"]
