{{
  config(
    pre_hook=[
      'alter table if exists "{{ this.schema }}"."{{ this.name }}" drop constraint if exists {{ this.name }}_pk',
      'alter table if exists "{{ this.schema }}"."{{ this.name }}" drop constraint if exists {{ this.name }}_uq',
    ],
    post_hook=[
      'alter table "{{ this.schema }}"."{{ this.name }}" add constraint {{ this.name }}_pk primary key (id)',
      'alter table "{{ this.schema }}"."{{ this.name }}" add constraint {{ this.name }}_uq unique (country, league, season)',
    ]
  )
}}
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
    )                                           as season
  FROM
    {{ ref('stg_match') }} stg
  JOIN
    {{ source('football-data', 'season') }} season USING (url)
  JOIN
    {{ source('football-data', 'league') }} league USING (division)
  JOIN
    {{ source('football-data', 'country') }} country USING (country_code)
)
SELECT DISTINCT
  {{ dbt_utils.surrogate_key(
      ['source', 'country', 'league', 'season']
    )
  }}                                          as id,
  country,
  league,
  season
FROM
  staging