{{
  config(
    pre_hook=[
      'alter table if exists "{{ this.schema }}"."{{ this.name }}" drop constraint if exists {{ this.name }}_pk',
    ],
    post_hook=[
      'alter table "{{ this.schema }}"."{{ this.name }}" add constraint {{ this.name }}_pk primary key (match_id, bookmaker_code)',
    ]
  )
}}
SELECT
  {{ dbt_utils.surrogate_key(
      ['source', 'date', 'home_team']
    )
  }}                                   as match_id,
  stg.bookmaker                        as bookmaker_code,
  stg.handicap                         as handicap,
  NULLIF(stg.home_odds, 0)             as home_odds,
  NULLIF(stg.away_odds, 0)             as away_odds,
  1/NULLIF(stg.home_odds, 0)           as home_implied_probability,
  1/NULLIF(stg.away_odds, 0)           as away_implied_probability,
  1/NULLIF(stg.home_odds, 0) +
  1/NULLIF(stg.away_odds, 0)           as overround,
  CASE
    WHEN NULLIF(stg.home_odds, 0) < NULLIF(stg.away_odds, 0)
    THEN 'H'
    ELSE 'A'
  END                                  as favourite,
  LEAST(
    NULLIF(stg.home_odds, 0),
    NULLIF(stg.away_odds, 0)
  )                                    as favourite_odds,
  GREATEST(
    1/NULLIF(stg.home_odds, 0),
    1/NULLIF(stg.away_odds, 0)
  )                                    as favourite_implied_probability,
  stg.bb_bookmakers                    as bb_bookmakers
FROM
  {{ ref('stg_match_asian_handicap') }} stg