SELECT
  raw.source::varchar(20)                       as source,
  TO_DATE(raw.date, 'DD/MM/YY')                 as date,
  raw.bookmaker::varchar(5)                     as bookmaker,
  raw.home_team::varchar(100)                   as home_team,
  NULLIF(raw.under_odds, '')::decimal(5,2)      as under_odds,
  NULLIF(raw.over_odds,  '')::decimal(5,2)      as over_odds,
  raw.away_team::varchar(100)                   as away_team,
  NULLIF(raw.bb_bookmakers, '')::smallint       as bb_bookmakers,
  raw.url
FROM
  {{ ref('raw_match_over_under') }} raw
WHERE
  raw.under_odds != 'null'