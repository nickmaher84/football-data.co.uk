SELECT
  json_value(raw.json, "$.Date")            as date,
  statistics.code                           as statistic,
  COALESCE(
    json_value(raw.json, "$.HT"),
    json_value(raw.json, "$.Home"),
    json_value(raw.json, "$.HomeTeam")
  )                                         as home_team,
  json_value(raw.json, concat("$.H", code)) as home_stat,
  json_value(raw.json, concat("$.A", code)) as away_stat,
  COALESCE(
    json_value(raw.json, "$.AT"),
    json_value(raw.json, "$.Away"),
    json_value(raw.json, "$.AwayTeam")
  )                                         as away_team,
  raw.url
FROM
  {{ source('football-data', 'raw') }} raw
JOIN
  {{ ref('statistics') }} statistics ON json_exists(raw.json, concat("$.H", code))
WHERE
  json_value(raw.json, "$.Date") != ''