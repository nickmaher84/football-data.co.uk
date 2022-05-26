SELECT
  raw.source::varchar(20)                       as source,
  TO_DATE(raw.date, 'DD/MM/YY')                 as date,
  raw.bookmaker::varchar(5)                     as bookmaker,
  raw.home_team::varchar(100)                   as home_team,
  NULLIF(raw.home_odds, '')::decimal(5,2)       as home_odds,
  NULLIF(raw.draw_odds, '')::decimal(5,2)       as draw_odds,
  NULLIF(raw.away_odds, '')::decimal(5,2)       as away_odds,
  raw.away_team::varchar(100)                   as away_team,
  NULLIF(raw.bb_bookmakers, '')::smallint       as bb_bookmakers,
  raw.url
FROM
  {{ ref('raw_match_1x2') }} raw
WHERE
  raw.home_odds != 'null'