

WITH
staging AS (
    SELECT
        ms.match_id,
        SUM(ms.home_stat+ms.away_stat) FILTER (WHERE s.name = 'Shots on Target') AS total_shots_on_target,
        SUM(ms.home_stat) FILTER (WHERE s.name = 'Shots on Target')              AS home_shots_on_target,
        SUM(ms.away_stat) FILTER (WHERE s.name = 'Shots on Target')              AS away_shots_on_target,
        SUM(ms.home_stat+ms.away_stat) FILTER (WHERE s.name = 'Shots')           AS total_shots,
        SUM(ms.home_stat) FILTER (WHERE s.name = 'Shots')                        AS home_shots,
        SUM(ms.away_stat) FILTER (WHERE s.name = 'Shots')                        AS away_shots
    FROM {{ ref('match_statistic') }} ms
    JOIN {{ ref('statistics') }} s ON ms.statistic_code = s.code
    GROUP BY ms.match_id
)

SELECT
    competition_id,
    count(*)                                   AS matches,
    SUM(home_goals+away_goals)                 AS total_goals,
    SUM(home_goals)                            AS home_goals,
    SUM(away_goals)                            AS away_goals,
    SUM(total_shots_on_target)                 AS total_shots_on_target,
    SUM(home_shots_on_target)                  AS home_shots_on_target,
    SUM(away_shots_on_target)                  AS away_shots_on_target,
    SUM(total_shots)                           AS total_shots,
    SUM(home_shots)                            AS home_shots,
    SUM(away_shots)                            AS away_shots,
    SUM(total_shots) * 0.45 >
        SUM(total_shots_on_target)             AS provider_type
FROM {{ ref('match') }} m
LEFT JOIN staging ON match_id = m.id
GROUP BY competition_id