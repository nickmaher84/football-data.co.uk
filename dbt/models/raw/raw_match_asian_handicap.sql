SELECT
    'football-data.co.uk'       as source,
    raw.json ->> 'Date'         as date,
    bookmakers.code             as bookmaker,
    COALESCE(
        raw.json ->> (code||'AH'),
        raw.json ->> 'BbAHh',
        raw.json ->> 'AHh'
    )                           as handicap,
    COALESCE(
        raw.json ->> 'HT',
        raw.json ->> 'Home',
        raw.json ->> 'HomeTeam'
    )                           as home_team,
    raw.json ->> (code||'AHH')  as home_odds,
    raw.json ->> (code||'AHA')  as away_odds,
    COALESCE(
        raw.json ->> 'AT',
        raw.json ->> 'Away',
        raw.json ->> 'AwayTeam'
    )                           as away_team,
    CASE
        WHEN bookmakers.name LIKE 'Betbrain %'
        THEN raw.json ->> 'BbAH'
    END                         as bb_bookmakers,
    raw.url
FROM
    {{ source('football-data', 'raw') }} raw
JOIN
    {{ ref('bookmakers') }} bookmakers ON raw.json ->> (code||'AHH') IS NOT NULL
WHERE
    raw.json ->> 'Date' != ''