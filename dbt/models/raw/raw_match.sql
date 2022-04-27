SELECT
  json_value(raw.json, "$.Country")    as country,
  json_value(raw.json, "$.League")     as league,
  json_value(raw.json, "$.Season")     as season,
  json_value(raw.json, "$.Date")       as date,
  json_value(raw.json, "$.Time")       as time,
  COALESCE(
    json_value(raw.json, "$.HT"),
    json_value(raw.json, "$.Home"),
    json_value(raw.json, "$.HomeTeam")
  )                                    as home_team,
  COALESCE(
    json_value(raw.json, "$.HG"),
    json_value(raw.json, "$.FTHG")
  )                                    as home_goals,
  COALESCE(
    json_value(raw.json, "$.Res"),
    json_value(raw.json, "$.FTR")
  )                                    as result,
  COALESCE(
    json_value(raw.json, "$.AG"),
    json_value(raw.json, "$.FTAG")
  )                                    as away_goals,
  COALESCE(
    json_value(raw.json, "$.AT"),
    json_value(raw.json, "$.Away"),
    json_value(raw.json, "$.AwayTeam")
  )                                    as away_team,
  json_value(raw.json, "$.HTHG")       as ht_home_goals,
  json_value(raw.json, "$.HTR")        as ht_result,
  json_value(raw.json, "$.HTAG")       as ht_away_goals,
  json_value(raw.json, "$.Referee")    as referee,
  json_value(raw.json, "$.Attendance") as attendance,
  raw.url
FROM
  {{ source('football-data', 'raw') }} raw
WHERE
  json_value(raw.json, "$.Date") != ''