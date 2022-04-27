SELECT
  json_value(raw.json, "$.Date")                       as date,
  bookmakers.code                                      as bookmaker,
  COALESCE(
    json_value(raw.json, "$.HT"),
    json_value(raw.json, "$.Home"),
    json_value(raw.json, "$.HomeTeam")
  )                                                    as home_team,
  json_value(raw.json, concat("$.""", code, "<2.5""")) as under_odds,
  json_value(raw.json, concat("$.""", code, ">2.5""")) as over_odds,
  COALESCE(
    json_value(raw.json, "$.AT"),
    json_value(raw.json, "$.Away"),
    json_value(raw.json, "$.AwayTeam")
  )                                                    as away_team,
  CASE
    WHEN bookmakers.name LIKE 'Betbrain %'
    THEN json_value(raw.json, "$.BbOU")
  END                                                  as bb_bookmakers,
  raw.url
FROM
  {{ source('football-data', 'raw') }} raw
JOIN
  {{ ref('bookmakers') }} bookmakers ON json_exists(raw.json, concat("$.""", code, ">2.5"""))
WHERE
  json_value(raw.json, "$.Date") != ''