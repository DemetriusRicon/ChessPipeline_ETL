-- =============================================================================
-- gold_rating_evolution
-- Gold Layer — Evolução de Rating Mensal por Categoria e Plataforma
--
-- Responde: "Como meu rating evoluiu mês a mês em cada categoria de tempo
-- (bullet, blitz, rapid), separado por plataforma (Chess.com / Lichess)?"
--
-- Lógica:
--   - Identifica minhas partidas (branco ou preto) via username fixo por plataforma
--   - Extrai MEU rating na partida (independente da cor que joguei)
--   - Agrupa por mês + categoria de tempo + plataforma
--   - Métrica do mês = MÉDIA do meu rating nas partidas daquele mês
--
-- =============================================================================

{{
  config(
    materialized = 'table',
    tags = ['gold', 'rating']
  )
}}

WITH chess_com_my_games AS (
    SELECT
        'chess_com' AS platform,
        time_class  AS time_category,
        DATE_TRUNC('month', end_time) AS month,
        CASE
            WHEN white_username = 'demetrius_ricon' THEN white_rating
            WHEN black_username = 'demetrius_ricon' THEN black_rating
        END                                          AS my_rating
    FROM {{ ref('stg_chess_com_games') }}
    WHERE white_username = 'demetrius_ricon'
       OR black_username = 'demetrius_ricon'
),

lichess_my_games AS (
    SELECT
        'lichess'                                  AS platform,
        speed                                       AS time_category,
        DATE_TRUNC('month', created_at)             AS month,
        CASE
            WHEN white_id = 'demetrius01' THEN white_rating
            WHEN black_id = 'demetrius01' THEN black_rating
        END                                          AS my_rating
    FROM {{ ref('stg_lichess_games') }}
    WHERE white_id = 'demetrius01'
       OR black_id = 'demetrius01'
),

unioned AS (
    SELECT * FROM chess_com_my_games
    UNION ALL
    SELECT * FROM lichess_my_games
),

filtered AS (
    -- Apenas categorias de interesse e ratings válidos
    SELECT *
    FROM unioned
    WHERE time_category IN ('bullet', 'blitz', 'rapid')
      AND my_rating IS NOT NULL
),

monthly_avg AS (
    SELECT
        platform,
        time_category,
        month,
        ROUND(AVG(my_rating), 0)   AS avg_rating,
        MIN(my_rating)             AS min_rating,
        MAX(my_rating)             AS max_rating,
        COUNT(*)                   AS games_played
    FROM filtered
    GROUP BY platform, time_category, month
),

with_variation AS (
    SELECT
        *,
        avg_rating - LAG(avg_rating) OVER (
            PARTITION BY platform, time_category
            ORDER BY month
        ) AS rating_change_vs_prev_month
    FROM monthly_avg
)

SELECT
    platform,
    time_category,
    month,
    avg_rating,
    min_rating,
    max_rating,
    games_played,
    rating_change_vs_prev_month

FROM with_variation
ORDER BY platform, time_category, month