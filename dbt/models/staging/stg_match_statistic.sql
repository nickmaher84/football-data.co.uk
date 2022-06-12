SELECT
    raw.source::varchar(20)                       as source,
    TO_DATE(raw.date, 'DD/MM/YY')                 as date,
    raw.statistic::varchar(3)                     as statistic,
    raw.home_team::varchar(100)                   as home_team,
    NULLIF(raw.home_stat, '')::integer            as home_stat,
    NULLIF(raw.away_stat, '')::integer            as away_stat,
    raw.away_team::varchar(100)                   as away_team,
    raw.url
FROM
    {{ ref('raw_match_statistic') }} raw