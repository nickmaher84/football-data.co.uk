{{
  config(
    unique_key = 'id',
    indexes=[
      {'columns': ['id'], 'unique': True},
      {'columns': ['name', 'country'], 'unique': True},
      {'columns': ['last_modified']},
    ]
  )
}}
WITH
staging AS (
  SELECT DISTINCT
    source,
    UNNEST(ARRAY[stg.home_team, stg.away_team]) as team_name,
    COALESCE(stg.country, country.country_name) as country,
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
      ['source', 'team_name', 'country']
    )
  }}                                          as id,
  team_name                                   as name,
  country                                     as country,
  max(last_modified)                          as last_modified
FROM
  staging
{% if is_incremental() %}
WHERE
  last_modified > (select max(this.last_modified) from {{ this }} this)
{% endif %}
GROUP BY
  source,
  team_name,
  country