{{ config(indexes=[{'columns': ['last_modified']}], unique_key='id') }}

SELECT
    {{ dbt_utils.surrogate_key(
        ['source', 'date', 'home_team', 'statistic']
    ) }}                                 as id,
    {{ dbt_utils.surrogate_key(
        ['source', 'date', 'home_team']
    ) }}                                 as match_id,
    stg.statistic                        as statistic_code,
    stg.home_stat                        as home_stat,
    stg.away_stat                        as away_stat,
    season.last_modified
FROM
    {{ ref('stg_match_statistic') }} stg
JOIN
    {{ source('football-data', 'season') }} season USING (url)
{% if is_incremental() %}
WHERE
    season.last_modified > (select max(this.last_modified) from {{ this }} this)
{% endif %}