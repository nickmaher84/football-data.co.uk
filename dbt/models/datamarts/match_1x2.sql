{{
    config(
        unique_key = 'id',
        indexes=[
            {'columns': ['id'], 'unique': True},
            {'columns': ['match_id', 'bookmaker_code'], 'unique': True},
            {'columns': ['last_modified']},
        ]
    )
}}
SELECT
    {{ dbt_utils.surrogate_key(
        ['source', 'date', 'home_team', 'bookmaker']
    ) }}                                 as id,
    {{ dbt_utils.surrogate_key(
        ['source', 'date', 'home_team']
    ) }}                                 as match_id,
    stg.bookmaker                        as bookmaker_code,
    NULLIF(stg.home_odds, 0)             as home_odds,
    NULLIF(stg.draw_odds, 0)             as draw_odds,
    NULLIF(stg.away_odds, 0)             as away_odds,
    1/NULLIF(stg.home_odds, 0)           as home_implied_probability,
    1/NULLIF(stg.draw_odds, 0)           as draw_implied_probability,
    1/NULLIF(stg.away_odds, 0)           as away_implied_probability,
    1/NULLIF(stg.home_odds, 0) +
    1/NULLIF(stg.draw_odds, 0) +
    1/NULLIF(stg.away_odds, 0)           as overround,
    CASE
        WHEN NULLIF(stg.home_odds, 0) < LEAST(NULLIF(stg.draw_odds, 0), NULLIF(stg.away_odds, 0))
        THEN 'H'
        WHEN NULLIF(stg.draw_odds, 0) < NULLIF(stg.away_odds, 0)
        THEN 'D'
        ELSE 'A'
    END                                  as favourite,
    LEAST(
        NULLIF(stg.home_odds, 0),
        NULLIF(stg.draw_odds, 0),
        NULLIF(stg.away_odds, 0)
    )                                    as favourite_odds,
    GREATEST(
        1/NULLIF(stg.home_odds, 0),
        1/NULLIF(stg.draw_odds, 0),
        1/NULLIF(stg.away_odds, 0)
    )                                    as favourite_implied_probability,
    stg.bb_bookmakers                    as bb_bookmakers,
    season.last_modified
FROM
    {{ ref('stg_match_1x2') }} stg
JOIN
    {{ source('football-data', 'season') }} season USING (url)
{% if is_incremental() %}
WHERE
    season.last_modified > (select max(this.last_modified) from {{ this }} this)
{% endif %}