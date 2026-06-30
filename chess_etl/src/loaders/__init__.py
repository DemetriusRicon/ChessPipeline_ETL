"""
src/loaders/__init__.py
"""
from .parquet_loader import save_games, read_parquet, CHESS_COM_SCHEMA, LICHESS_SCHEMA

__all__ = ["save_games", "read_parquet", "CHESS_COM_SCHEMA", "LICHESS_SCHEMA"]

