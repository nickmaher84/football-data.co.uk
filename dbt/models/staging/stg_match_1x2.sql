SELECT
  STR_TO_DATE(raw.date, '%d/%m/%Y')                as date,
  CAST(raw.bookmaker as varchar(5))                as bookmaker,
  CAST(raw.home_team as varchar(100))              as home_team,
  CAST(NULLIF(raw.home_odds, '') as decimal(5,2))  as home_odds,
  CAST(NULLIF(raw.draw_odds, '') as decimal(5,2))  as draw_odds,
  CAST(NULLIF(raw.away_odds, '') as decimal(5,2))  as away_odds,
  CAST(raw.away_team as varchar(100))              as away_team,
  CAST(NULLIF(raw.bb_bookmakers, '') as integer)   as bb_bookmakers,
  raw.url
FROM
  {{ ref('raw_match_1x2') }} raw
WHERE
  raw.home_odds != 'null'