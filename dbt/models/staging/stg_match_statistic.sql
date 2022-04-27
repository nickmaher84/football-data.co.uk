SELECT
  STR_TO_DATE(raw.date, '%d/%m/%Y')                as date,
  CAST(raw.statistic as varchar(3))                as statistic,
  CAST(raw.home_team as varchar(100))              as home_team,
  CAST(NULLIF(raw.home_stat, '') as integer)       as home_stat,
  CAST(NULLIF(raw.away_stat, '') as integer)       as away_stat,
  CAST(raw.away_team as varchar(100))              as away_team,
  raw.url
FROM
  {{ ref('raw_match_statistic') }} raw