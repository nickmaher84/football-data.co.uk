{{ config(indexes=[{'columns': ['last_modified']}], unique_key='id') }}

SELECT
    {{ dbt_utils.surrogate_key(
        ['source', 'date', 'home_team', 'bookmaker']
    ) }}                                 as id,
    {{ dbt_utils.surrogate_key(
        ['source', 'date', 'home_team']
    ) }}                                 as match_id,
    stg.bookmaker                        as bookmaker_code,
    NULLIF(stg.under_odds, 0)            as under_odds,
    NULLIF(stg.over_odds, 0)             as over_odds,
    1/NULLIF(stg.under_odds, 0)          as under_implied_probability,
    1/NULLIF(stg.over_odds, 0)           as over_implied_probability,
    1/NULLIF(stg.under_odds, 0) +
    1/NULLIF(stg.over_odds, 0)           as overround,
    CASE
        WHEN NULLIF(stg.over_odds, 0) < NULLIF(stg.under_odds, 0)
        THEN 'O'
        ELSE 'U'
    END                                  as favourite,
    LEAST(
        NULLIF(stg.under_odds, 0),
        NULLIF(stg.over_odds, 0)
    )                                    as favourite_odds,
    GREATEST(
        1/NULLIF(stg.under_odds, 0),
        1/NULLIF(stg.over_odds, 0)
    )                                    as favourite_implied_probability,
    stg.bb_bookmakers                    as bb_bookmakers,
    season.last_modified
FROM
    {{ ref('stg_match_over_under') }} stg
JOIN
    {{ source('football-data', 'season') }} season USING (url)
{% if is_incremental() %}
WHERE
    season.last_modified > (select max(this.last_modified) from {{ this }} this)
{% endif %}