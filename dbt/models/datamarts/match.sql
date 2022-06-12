{{
  config(
    unique_key = 'id',
    indexes=[
      {'columns': ['id'], 'unique': True},
      {'columns': ['date', 'home_team_id'], 'unique': True},
      {'columns': ['date', 'away_team_id'], 'unique': True},
      {'columns': ['last_modified']},
    ]
  )
}}
WITH
staging AS (
  SELECT
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
    stg.date                                    as date,
    stg.time                                    as time,
    stg.home_team                               as home_team,
    stg.home_goals                              as home_goals,
    stg.result                                  as result,
    stg.away_goals                              as away_goals,
    stg.away_team                               as away_team,
    stg.ht_home_goals                           as ht_home_goals,
    stg.ht_result                               as ht_result,
    stg.ht_away_goals                           as ht_away_goals,
    stg.referee                                 as referee_name,
    stg.attendance                              as attendance,
    season.last_modified
  FROM
    {{ ref('stg_match') }} stg
  JOIN
    {{ source('football-data', 'season') }} season USING (url)
  JOIN
    {{ source('football-data', 'league') }} league USING (division)
  JOIN
    {{ source('football-data', 'country') }} country USING (country_code)
{% if is_incremental() %}
  WHERE
    season.last_modified > (select max(this.last_modified) from {{ this }} this)
{% endif %}
)
SELECT
  {{ dbt_utils.surrogate_key(
      ['source', 'date', 'home_team']
    )
  }}                                     as id,
  {{ dbt_utils.surrogate_key(
      ['source', 'country', 'league', 'season']
    )
  }}                                     as competition_id,
  date,
  time,
  {{ dbt_utils.surrogate_key(
      ['source', 'home_team', 'country']
    )
  }}                                     as home_team_id,
  home_goals,
  result,
  away_goals,
  {{ dbt_utils.surrogate_key(
      ['source', 'away_team', 'country']
    )
  }}                                     as away_team_id,
  ht_home_goals,
  ht_result,
  ht_away_goals,
  CASE referee_name
    WHEN NOT NULL
    THEN {{ dbt_utils.surrogate_key(['source', 'referee_name', 'country']) }}
  END                                    as referee_id,
  attendance,
  last_modified
FROM
  staging