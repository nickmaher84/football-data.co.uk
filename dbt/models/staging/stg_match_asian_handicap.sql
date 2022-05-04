SELECT
  TO_DATE(raw.date, '%d/%m/%Y')                    as date,
  CAST(raw.bookmaker as varchar(5))                as bookmaker,
  CAST(raw.home_team as varchar(100))              as home_team,
  CAST(NULLIF(raw.home_odds, '') as decimal(5,2))  as home_odds,
  CAST(NULLIF(raw.away_odds, '') as decimal(5,2))  as away_odds,
  CAST(raw.away_team as varchar(100))              as away_team,
  CASE
    WHEN raw.handicap like '%,%'
    THEN (CAST(SPLIT_PART(raw.handicap, ',', -1) as decimal(2,1))
         +CAST(SPLIT_PART(raw.handicap, ',', +1) as decimal(2,1))
         ) * 0.5
    ELSE CAST(NULLIF(raw.handicap, '') as decimal(6,3))
  END                                              as handicap,
  CAST(
    CAST(NULLIF(REPLACE(REPLACE(raw.bb_bookmakers, CHAR(194), ''), CHAR(160), ''), '') as float) as smallint
  )                                                as bb_bookmakers,
  raw.url
FROM
  {{ ref('raw_match_asian_handicap') }} raw
WHERE
  raw.handicap != 'null'