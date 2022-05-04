SELECT
  TO_DATE(raw.date, '%d/%m/%Y')                    as date,
  CAST(raw.bookmaker as varchar(5))                as bookmaker,
  CAST(raw.home_team as varchar(100))              as home_team,
  CAST(NULLIF(raw.under_odds, '') as decimal(5,2)) as under_odds,
  CAST(NULLIF(raw.over_odds,  '') as decimal(5,2)) as over_odds,
  CAST(raw.away_team as varchar(100))              as away_team,
  CAST(NULLIF(raw.bb_bookmakers, '') as smallint)  as bb_bookmakers,
  raw.url
FROM
  {{ ref('raw_match_over_under') }} raw
WHERE
  raw.under_odds != 'null'