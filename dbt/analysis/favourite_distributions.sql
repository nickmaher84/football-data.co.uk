SELECT
    bookmakers.name                         AS bookmaker,
    bookmakers.closing                      AS is_closing,
    m.id                                    AS match_id,
    m.date                                  AS match_date,
    CASE match_1x2.favourite
        WHEN 'H' THEN home_team.name
        WHEN 'A' THEN away_team.name
    END                                     AS favourite,
    CASE match_1x2.favourite
        WHEN 'H' THEN away_team.name
        WHEN 'A' THEN home_team.name
    END                                     AS underdog,
    match_1x2.favourite_odds                AS decimal_odds,
    match_1x2.favourite_implied_probability AS probability,
    match_1x2.favourite = m.result          AS winner,
    match_1x2.overround
FROM
    {{ ref('match_1x2') }} match_1x2
JOIN
    {{ ref('match') }} m ON match_1x2.match_id = m.id
LEFT JOIN
    {{ ref('team') }} home_team ON m.home_team_id = home_team.id
LEFT JOIN
    {{ ref('team') }} away_team ON m.away_team_id = away_team.id
JOIN
    {{ ref('competition') }} competition ON m.competition_id = competition.id
JOIN
    {{ ref('bookmakers') }} bookmakers ON match_1x2.bookmaker_code = bookmakers.code
WHERE
    competition.country = 'England'
AND
    competition.league = 'Premier League'
AND
    competition.season = '2021/2022'
AND
    match_1x2.favourite_odds IS NOT NULL
ORDER BY
    bookmakers.name,
    closing,
    date,
    favourite;