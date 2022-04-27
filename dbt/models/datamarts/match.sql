SELECT
  season.division                        as division,
  COALESCE(stg.country, country.country) as country,
  COALESCE(stg.league, league.league)    as league,
  COALESCE(stg.season, season.season)    as season,
  stg.date                               as date,
  stg.time                               as time,
  stg.home_team                          as home_team,
  stg.home_goals                         as home_goals,
  stg.result                             as result,
  stg.away_goals                         as away_goals,
  stg.away_team                          as away_team,
  stg.ht_home_goals                      as ht_home_goals,
  stg.ht_result                          as ht_result,
  stg.ht_away_goals                      as ht_away_goals,
  stg.referee                            as referee,
  stg.attendance                         as attendance,
  stg.url
FROM
  {{ ref('stg_match') }} stg
JOIN
  {{ source('football-data', 'season') }} season USING (url)
JOIN
  {{ source('football-data', 'league') }} league USING (division)
JOIN
  {{ source('football-data', 'country') }} country USING (country_code)