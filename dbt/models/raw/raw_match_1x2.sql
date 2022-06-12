SELECT
    'football-data.co.uk'       as source,
    raw.json ->> 'Date'         as date,
    bookmakers.code             as bookmaker,
    COALESCE(
        raw.json ->> 'HT',
        raw.json ->> 'Home',
        raw.json ->> 'HomeTeam'
    )                           as home_team,
    raw.json ->> (code||'H')    as home_odds,
    raw.json ->> (code||'D')    as draw_odds,
    raw.json ->> (code||'A')    as away_odds,
    COALESCE(
        raw.json ->> 'AT',
        raw.json ->> 'Away',
        raw.json ->> 'AwayTeam'
    )                           as away_team,
    CASE
        WHEN bookmakers.name LIKE 'Betbrain %'
        THEN raw.json ->> 'Bb1x2'
    END                         as bb_bookmakers,
    raw.url
FROM
    {{ source('football-data', 'raw') }} raw
JOIN
    {{ ref('bookmakers') }} bookmakers ON raw.json ->> (code||'H') IS NOT NULL
WHERE
    raw.json ->> 'Date' != ''