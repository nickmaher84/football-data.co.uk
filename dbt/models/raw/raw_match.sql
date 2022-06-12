SELECT
    'football-data.co.uk'       as source,
    raw.json ->> 'Country'      as country,
    raw.json ->> 'League'       as league,
    raw.json ->> 'Season'       as season,
    raw.json ->> 'Date'         as date,
    raw.json ->> 'Time'         as time,
    COALESCE(
        raw.json ->> 'HT',
        raw.json ->> 'Home',
        raw.json ->> 'HomeTeam'
    )                           as home_team,
    COALESCE(
        raw.json ->> 'HG',
        raw.json ->> 'FTHG'
    )                           as home_goals,
    COALESCE(
        raw.json ->> 'Res',
        raw.json ->> 'FTR'
    )                           as result,
    COALESCE(
        raw.json ->> 'AG',
        raw.json ->> 'FTAG'
    )                           as away_goals,
    COALESCE(
        raw.json ->> 'AT',
        raw.json ->> 'Away',
        raw.json ->> 'AwayTeam'
    )                           as away_team,
    raw.json ->> 'HTHG'         as ht_home_goals,
    raw.json ->> 'HTR'          as ht_result,
    raw.json ->> 'HTAG'         as ht_away_goals,
    raw.json ->> 'Referee'      as referee,
    raw.json ->> 'Attendance'   as attendance,
    raw.url
FROM
    {{ source('football-data', 'raw') }} raw
WHERE
    raw.json ->> 'Date' != ''