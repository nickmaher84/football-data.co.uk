SELECT
  raw.source::varchar(20)                       as source,
  TO_DATE(raw.date, 'DD/MM/YY')                 as date,
  raw.bookmaker::varchar(5)                     as bookmaker,
  CASE
    WHEN raw.handicap like '%,%'
    THEN (SPLIT_PART(raw.handicap, ',', 1)::decimal(2,1)
         +SPLIT_PART(raw.handicap, ',', 2)::decimal(2,1)
         ) * 0.5
    ELSE NULLIF(raw.handicap, '')::decimal(6,3)
  END                                           as handicap,
  raw.home_team::varchar(100)                   as home_team,
  NULLIF(raw.home_odds, '')::decimal(5,2)       as home_odds,
  NULLIF(raw.away_odds, '')::decimal(5,2)       as away_odds,
  raw.away_team::varchar(100)                   as away_team,
  NULLIF(
    REPLACE(REPLACE(raw.bb_bookmakers, CHR(194), ''), CHR(160), '')
  , '')::float::smallint                        as bb_bookmakers,
  raw.url
FROM
  {{ ref('raw_match_asian_handicap') }} raw
WHERE
  raw.handicap != 'null'