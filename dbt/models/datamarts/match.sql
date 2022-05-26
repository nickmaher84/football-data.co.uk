{{
  config(
    pre_hook=[
      'alter table if exists "{{ this.schema }}"."{{ this.name }}" drop constraint if exists {{ this.name }}_pk',
      'alter table if exists "{{ this.schema }}"."{{ this.name }}" drop constraint if exists {{ this.name }}_uq_home',
      'alter table if exists "{{ this.schema }}"."{{ this.name }}" drop constraint if exists {{ this.name }}_uq_away',
    ],
    post_hook=[
      'alter table "{{ this.schema }}"."{{ this.name }}" add constraint {{ this.name }}_pk primary key (id)',
      'alter table "{{ this.schema }}"."{{ this.name }}" add constraint {{ this.name }}_uq_home unique (date, home_team_id)',
      'alter table "{{ this.schema }}"."{{ this.name }}" add constraint {{ this.name }}_uq_away unique (date, away_team_id)',
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
    stg.referee                                 as referee,
    stg.attendance                              as attendance
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
  referee,
  attendance
FROM
  staging