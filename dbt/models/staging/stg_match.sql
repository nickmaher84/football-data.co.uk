SELECT
  raw.source::varchar(20)                       as source,
  trim(raw.country)::varchar(20)                as country,
  trim(raw.league)::varchar(100)                as league,
  trim(raw.season)::varchar(100)                as season,
  TO_DATE(raw.date, 'DD/MM/YY')                 as date,
  TO_TIMESTAMP(raw.time, 'HH24:mi')::time       as time,
  trim(raw.home_team)::varchar(100)             as home_team,
  NULLIF(raw.home_goals, '')::smallint          as home_goals,
  raw.result::char                              as result,
  NULLIF(raw.away_goals, '')::smallint          as away_goals,
  trim(raw.away_team)::varchar(100)             as away_team,
  NULLIF(raw.ht_home_goals, '')::smallint       as ht_home_goals,
  raw.ht_result::char                           as ht_result,
  NULLIF(raw.ht_away_goals, '')::smallint       as ht_away_goals,
  trim(raw.referee)::varchar(100)               as referee,
  NULLIF(NULLIF(raw.attendance, 'NA'), '')::int as attendance,
  raw.url
FROM
  {{ ref('raw_match') }} raw