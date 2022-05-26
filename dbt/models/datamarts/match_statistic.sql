{{
  config(
    pre_hook=[
      'alter table if exists "{{ this.schema }}"."{{ this.name }}" drop constraint if exists {{ this.name }}_pk',
    ],
    post_hook=[
      'alter table "{{ this.schema }}"."{{ this.name }}" add constraint {{ this.name }}_pk primary key (match_id, statistic_code)',
    ]
  )
}}
SELECT
  {{ dbt_utils.surrogate_key(
      ['source', 'date', 'home_team']
    )
  }}                                   as match_id,
  stg.statistic                        as statistic_code,
  stg.home_stat                        as home_stat,
  stg.away_stat                        as away_stat
FROM
  {{ ref('stg_match_statistic') }} stg