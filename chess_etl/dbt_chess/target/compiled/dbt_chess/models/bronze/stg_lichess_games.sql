-- =============================================================================
-- stg_lichess_games
-- Bronze Layer — Partidas Lichess.org
--
-- Lê os arquivos Parquet da Bronze Layer via DuckDB e aplica:
--   - Validação de campos obrigatórios
--   - Cast explícito de tipos
--   - Normalização de campos aninhados do ND-JSON (players, clock)
--   - Padronização de colunas para modelo canônico
-- =============================================================================



WITH source AS (
    SELECT *
    FROM read_parquet(
        '/opt/airflow/data/bronze/lichess/**/*.parquet',
        union_by_name = true
    )
),

validated AS (
    SELECT
        -- Identificação
        CAST(game_id        AS VARCHAR)     AS game_id,

        -- Metadados do jogo
        CAST(variant        AS VARCHAR)     AS variant,
        CAST(speed          AS VARCHAR)     AS speed,
        CAST(perf           AS VARCHAR)     AS perf,
        CAST(status         AS VARCHAR)     AS status,

        -- Temporal
        CAST(created_at     AS TIMESTAMPTZ) AS created_at,
        CAST(last_move_at   AS TIMESTAMPTZ) AS last_move_at,

        -- Metadado de ingestão
        CAST(ingestion_ts   AS TIMESTAMPTZ) AS ingestion_ts,

        -- Flags
        CAST(rated          AS BOOLEAN)     AS is_rated,

        -- Jogador Branco
        LOWER(CAST(white_id     AS VARCHAR)) AS white_id,
        CAST(white_rating       AS INTEGER)  AS white_rating,
        CAST(white_result       AS VARCHAR)  AS white_result,

        -- Jogador Preto
        LOWER(CAST(black_id     AS VARCHAR)) AS black_id,
        CAST(black_rating       AS INTEGER)  AS black_rating,
        CAST(black_result       AS VARCHAR)  AS black_result,

        -- Movimentos
        CAST(moves          AS VARCHAR)     AS moves,

        -- Relógio
        CAST(clock_initial  AS INTEGER)     AS clock_initial_secs,
        CAST(clock_increment AS INTEGER)    AS clock_increment_secs,

        -- Duração total da partida em segundos
        CASE
            WHEN last_move_at IS NOT NULL AND created_at IS NOT NULL
            THEN EPOCH(CAST(last_move_at AS TIMESTAMPTZ)) -
                 EPOCH(CAST(created_at   AS TIMESTAMPTZ))
            ELSE NULL
        END AS game_duration_secs

    FROM source

    -- Filtro de qualidade: game_id obrigatório
    WHERE game_id IS NOT NULL
    
      AND created_at >= '2026-06-11'::timestamptz
      AND created_at < ('2026-06-18'::timestamptz + interval '1 day')
    
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
    variant,
    speed,
    perf,
    status,
    created_at,
    last_move_at,
    game_duration_secs,
    ingestion_ts,
    is_rated,
    white_id,
    white_rating,
    white_result,
    black_id,
    black_rating,
    black_result,
    moves,
    clock_initial_secs,
    clock_increment_secs

FROM deduped
WHERE row_num = 1