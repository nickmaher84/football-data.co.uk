SELECT
  CAST(raw.country as varchar(20))                 as country,
  CAST(raw.league as varchar(100))                 as league,
  CAST(raw.season as varchar(100))                 as season,
  STR_TO_DATE(raw.date, '%d/%m/%Y')                as date,
  STR_TO_DATE(raw.time, '%H:%i')                   as time,
  CAST(raw.home_team as varchar(100))              as home_team,
  CAST(
    NULLIF(raw.home_goals, '') as unsigned
  )                                                as home_goals,
  CAST(raw.result as char)                         as result,
  CAST(
    NULLIF(raw.away_goals, '') as unsigned
  )                                                as away_goals,
  CAST(raw.away_team as varchar(100))              as away_team,
  CAST(
    NULLIF(raw.ht_home_goals, '') as unsigned
  )                                                as ht_home_goals,
  CAST(raw.ht_result as char)                      as ht_result,
  CAST(
    NULLIF(raw.ht_away_goals, '') as unsigned
  )                                                as ht_away_goals,
  CAST(raw.referee as varchar(100))                as referee,
  CAST(
    NULLIF(NULLIF(raw.attendance, 'NA'), '') as unsigned
  )                                                as attendance,
  raw.url
FROM
  {{ ref('raw_match') }} raw