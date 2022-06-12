SELECT
    'football-data.co.uk'       as source,
    raw.json ->> 'Date'         as date,
    bookmakers.code             as bookmaker,
    COALESCE(
        raw.json ->> 'HT',
        raw.json ->> 'Home',
        raw.json ->> 'HomeTeam'
    )                           as home_team,
    raw.json ->> (code||'<2.5') as under_odds,
    raw.json ->> (code||'>2.5') as over_odds,
    COALESCE(
        raw.json ->> 'AT',
        raw.json ->> 'Away',
        raw.json ->> 'AwayTeam'
    )                           as away_team,
    CASE
        WHEN bookmakers.name LIKE 'Betbrain %'
        THEN raw.json ->> 'BbOU'
    END                         as bb_bookmakers,
    raw.url
FROM
    {{ source('football-data', 'raw') }} raw
JOIN
    {{ ref('bookmakers') }} bookmakers ON raw.json ->> (code||'>2.5') IS NOT NULL
WHERE
    raw.json ->> 'Date' != ''