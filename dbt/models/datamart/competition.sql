{{ config(indexes=[{'columns': ['last_modified']}], unique_key='id') }}

WITH
staging AS (
    SELECT DISTINCT
        stg.source                                  as source,
        COALESCE(
        stg.country,
        country.country_name
        )                                           as country,
        COALESCE(
            stg.league,
            league.league_name
        )                                           as league,
        COALESCE(
            stg.season,
            replace(season.season_name, 'Season ', '')
        )                                           as season,
        season.last_modified
    FROM
        {{ ref('stg_match') }} stg
    JOIN
        {{ source('football-data', 'season') }} season USING (url)
    JOIN
        {{ source('football-data', 'league') }} league USING (division)
    JOIN
        {{ source('football-data', 'country') }} country USING (country_code)
)
SELECT
    {{ dbt_utils.surrogate_key(
        ['source', 'country', 'league', 'season']
    ) }}                                          as id,
    country,
    league,
    season,
    last_modified
FROM
    staging
{% if is_incremental() %}
WHERE
    last_modified > (select max(this.last_modified) from {{ this }} this)
{% endif %}