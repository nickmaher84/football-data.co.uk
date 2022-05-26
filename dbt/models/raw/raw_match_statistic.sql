SELECT
  'football-data.co.uk'       as source,
  raw.json ->> 'Date'         as date,
  statistics.code             as statistic,
  COALESCE(
    raw.json ->> 'HT',
    raw.json ->> 'Home',
    raw.json ->> 'HomeTeam'
  )                           as home_team,
  raw.json ->> ('H'||code)    as home_stat,
  raw.json ->> ('A'||code)    as away_stat,
  COALESCE(
    raw.json ->> 'AT',
    raw.json ->> 'Away',
    raw.json ->> 'AwayTeam'
  )                           as away_team,
  raw.url
FROM
  {{ source('football-data', 'raw') }} raw
JOIN
  {{ ref('statistics') }} statistics ON raw.json ->> ('H'||code) IS NOT NULL
WHERE
  raw.json ->> 'Date' != ''