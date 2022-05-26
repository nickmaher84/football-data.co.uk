{{
  config(
    pre_hook=[
      'alter table if exists "{{ this.schema }}"."{{ this.name }}" drop constraint if exists {{ this.name }}_pk',
      'alter table if exists "{{ this.schema }}"."{{ this.name }}" drop constraint if exists {{ this.name }}_uq',
    ],
    post_hook=[
      'alter table "{{ this.schema }}"."{{ this.name }}" add constraint {{ this.name }}_pk primary key (id)',
      'alter table "{{ this.schema }}"."{{ this.name }}" add constraint {{ this.name }}_uq unique (name, country)',
    ]
  )
}}
WITH
staging AS (
  SELECT DISTINCT
    source,
    UNNEST(ARRAY[stg.home_team, stg.away_team]) as team_name,
    COALESCE(stg.country, country.country_name) as country
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
      ['source', 'team_name', 'country']
    )
  }}                                          as id,
  team_name                                   as name,
  country
FROM
  staging