-- =============================================================================
-- stg_chess_com_games
-- Bronze Layer — Partidas Chess.com
--
-- Lê os arquivos Parquet da Bronze Layer via DuckDB e aplica:
--   - Validação de campos obrigatórios
--   - Cast explícito de tipos
--   - Padronização de colunas para modelo canônico
-- =============================================================================



WITH source AS (
    SELECT *
    FROM read_parquet(
        '/opt/airflow/data/bronze/chess_com/**/*.parquet',
        union_by_name = true
    )
),

validated AS (
    SELECT
        -- Identificação
        CAST(game_id        AS VARCHAR)     AS game_id,
        CAST(url            AS VARCHAR)     AS url,

        -- Conteúdo da partida
        CAST(pgn            AS VARCHAR)     AS pgn,
        CAST(time_control   AS VARCHAR)     AS time_control,
        CAST(time_class     AS VARCHAR)     AS time_class,
        CAST(rules          AS VARCHAR)     AS rules,

        -- Temporal
        CAST(end_time       AS TIMESTAMPTZ) AS end_time,
        CAST(source_month   AS VARCHAR)     AS source_month,

        -- Metadado de ingestão
        CAST(ingestion_ts   AS TIMESTAMPTZ) AS ingestion_ts,

        -- Flags
        CAST(rated          AS BOOLEAN)     AS is_rated,

        -- Jogador Branco
        LOWER(CAST(white_username AS VARCHAR)) AS white_username,
        CAST(white_rating         AS INTEGER)  AS white_rating,
        CAST(white_result         AS VARCHAR)  AS white_result,

        -- Jogador Preto
        LOWER(CAST(black_username AS VARCHAR)) AS black_username,
        CAST(black_rating         AS INTEGER)  AS black_rating,
        CAST(black_result         AS VARCHAR)  AS black_result

    FROM source

    -- Filtro de qualidade: apenas registros com game_id e pelo menos um jogador
    WHERE game_id IS NOT NULL
      AND (white_username IS NOT NULL OR black_username IS NOT NULL)
),

deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY game_id
            ORDER BY ingestion_ts DESC
        ) AS row_num
    FROM validated
)

SELECT
    game_id,
    url,
    pgn,
    time_control,
    time_class,
    rules,
    end_time,
    source_month,
    ingestion_ts,
    is_rated,
    white_username,
    white_rating,
    white_result,
    black_username,
    black_rating,
    black_result

FROM deduped
WHERE row_num = 1