"""
Parquet Loader — Bronze Layer.

Salva DataFrames em Parquet com:
  - Particionamento por YYYY/MM
  - Schema enforcement via PyArrow
  - Append incremental (não sobrescreve partições existentes)
  - Compressão snappy (padrão)
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

# ─── Schemas PyArrow Bronze ───────────────────────────────────────────────────

CHESS_COM_SCHEMA = pa.schema([
    pa.field("game_id", pa.string()),
    pa.field("url", pa.string()),
    pa.field("pgn", pa.string()),
    pa.field("time_control", pa.string()),
    pa.field("end_time", pa.timestamp("us", tz="UTC")),
    pa.field("rated", pa.bool_()),
    pa.field("time_class", pa.string()),
    pa.field("rules", pa.string()),
    pa.field("white_username", pa.string()),
    pa.field("white_rating", pa.int64()),
    pa.field("white_result", pa.string()),
    pa.field("black_username", pa.string()),
    pa.field("black_rating", pa.int64()),
    pa.field("black_result", pa.string()),
    pa.field("ingestion_ts", pa.timestamp("us", tz="UTC")),
    pa.field("source_month", pa.string()),
])

LICHESS_SCHEMA = pa.schema([
    pa.field("game_id", pa.string()),
    pa.field("rated", pa.bool_()),
    pa.field("variant", pa.string()),
    pa.field("speed", pa.string()),
    pa.field("perf", pa.string()),
    pa.field("created_at", pa.timestamp("us", tz="UTC")),
    pa.field("last_move_at", pa.timestamp("us", tz="UTC")),
    pa.field("status", pa.string()),
    pa.field("white_id", pa.string()),
    pa.field("white_rating", pa.int64()),
    pa.field("white_result", pa.string()),
    pa.field("black_id", pa.string()),
    pa.field("black_rating", pa.int64()),
    pa.field("black_result", pa.string()),
    pa.field("moves", pa.string()),
    pa.field("clock_initial", pa.int64()),
    pa.field("clock_increment", pa.int64()),
    pa.field("ingestion_ts", pa.timestamp("us", tz="UTC")),
])


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _df_to_arrow_table(df: pd.DataFrame, schema: pa.Schema) -> pa.Table:
    """Converte DataFrame para PyArrow Table aplicando o schema Bronze."""
    # Garante que colunas de timestamp com timezone sejam convertidas corretamente
    for field in schema:
        if pa.types.is_timestamp(field.type) and field.name in df.columns:
            col = pd.to_datetime(df[field.name], utc=True, errors="coerce")
            df = df.copy()
            df[field.name] = col

    return pa.Table.from_pandas(df, schema=schema, preserve_index=False)


# ─── Funções públicas ─────────────────────────────────────────────────────────

def save_chess_com_games(
    df: pd.DataFrame,
    source_month: str,
    base_path: str | None = None,
    username: str = "demetriusricon",
) -> Path:
    """
    Salva partidas Chess.com na Bronze Layer.

    Caminho: {base_path}/chess_com/{username}/{YYYY}/{MM}/games.parquet

    Args:
        df: DataFrame normalizado de partidas Chess.com.
        source_month: String 'YYYY-MM'.
        base_path: Diretório raiz da Bronze Layer.
        username: Username do jogador (para particionamento).

    Returns:
        Path do arquivo Parquet criado.
    """
    if df.empty:
        logger.info("DataFrame vazio — nada a salvar para chess_com/%s.", source_month)
        return Path()

    base = Path(base_path or os.getenv("BRONZE_BASE_PATH", "./data/bronze"))
    year, month = source_month.split("-")
    out_dir = base / "chess_com" / username / year / month
    _ensure_dir(out_dir)
    out_path = out_dir / "games.parquet"

    table = _df_to_arrow_table(df, CHESS_COM_SCHEMA)

    if out_path.exists():
        # Append: lê existente, concatena e re-salva (deduplicando por game_id)
        existing = pq.read_table(str(out_path))
        combined = pa.concat_tables([existing, table])
        combined_df = combined.to_pandas().drop_duplicates(subset=["game_id"])
        table = _df_to_arrow_table(combined_df, CHESS_COM_SCHEMA)
        logger.info("Append em %s (%d registros únicos)", out_path, len(combined_df))
    else:
        logger.info("Criando %s (%d registros)", out_path, len(df))

    pq.write_table(table, str(out_path), compression="snappy")
    logger.info("✅ Parquet salvo: %s", out_path)
    return out_path


def save_lichess_games(
    df: pd.DataFrame,
    source_month: str,
    base_path: str | None = None,
    username: str = "Demetrius01",
) -> Path:
    """
    Salva partidas Lichess na Bronze Layer.

    Caminho: {base_path}/lichess/{username}/{YYYY}/{MM}/games.parquet

    Args:
        df: DataFrame normalizado de partidas Lichess.
        source_month: String 'YYYY-MM'.
        base_path: Diretório raiz da Bronze Layer.
        username: Username do jogador.

    Returns:
        Path do arquivo Parquet criado.
    """
    if df.empty:
        logger.info("DataFrame vazio — nada a salvar para lichess/%s.", source_month)
        return Path()

    base = Path(base_path or os.getenv("BRONZE_BASE_PATH", "./data/bronze"))
    year, month = source_month.split("-")
    out_dir = base / "lichess" / username.lower() / year / month
    _ensure_dir(out_dir)
    out_path = out_dir / "games.parquet"

    table = _df_to_arrow_table(df, LICHESS_SCHEMA)

    if out_path.exists():
        existing = pq.read_table(str(out_path))
        combined = pa.concat_tables([existing, table])
        combined_df = combined.to_pandas().drop_duplicates(subset=["game_id"])
        table = _df_to_arrow_table(combined_df, LICHESS_SCHEMA)
        logger.info("Append em %s (%d registros únicos)", out_path, len(combined_df))
    else:
        logger.info("Criando %s (%d registros)", out_path, len(df))

    pq.write_table(table, str(out_path), compression="snappy")
    logger.info("✅ Parquet salvo: %s", out_path)
    return out_path


def read_parquet(path: str | Path) -> pd.DataFrame:
    """Lê um arquivo Parquet da Bronze Layer."""
    return pq.read_table(str(path)).to_pandas()
