SELECT
    raw.source::varchar(20)                       as source,
    TRIM(raw.country)::varchar(20)                as country,
    TRIM(raw.league)::varchar(100)                as league,
    TRIM(raw.season)::varchar(100)                as season,
    TO_DATE(raw.date, 'DD/MM/YY')                 as date,
    TO_TIMESTAMP(raw.time, 'HH24:mi')::time       as time,
    TRIM(raw.home_team)::varchar(100)             as home_team,
    NULLIF(raw.home_goals, '')::smallint          as home_goals,
    NULLIF(raw.result, '')::char                  as result,
    NULLIF(raw.away_goals, '')::smallint          as away_goals,
    TRIM(raw.away_team)::varchar(100)             as away_team,
    NULLIF(raw.ht_home_goals, '')::smallint       as ht_home_goals,
    NULLIF(raw.ht_result, '')::char               as ht_result,
    NULLIF(raw.ht_away_goals, '')::smallint       as ht_away_goals,
    NULLIF(TRIM(
      TRIM(raw.referee, chr(160))
    ), '')::varchar(100)                          as referee,
    NULLIF(NULLIF(raw.attendance, 'NA'), '')::int as attendance,
    raw.url
FROM
    {{ ref('raw_match') }} raw
