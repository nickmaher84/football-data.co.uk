{% set seasons = dbt_utils.get_column_values(
    table=ref('german_leagues'),
    column='season',
    order_by='season'
) %}
{% set leagues = dbt_utils.get_column_values(
    table=ref('german_leagues'),
    column='league',
    order_by='league'
) %}
WITH
staging AS (
    SELECT
        full_name as referee,
        league,
        season,
        count(*)  as matches
    FROM
        {{ ref('referee') }} referee
    JOIN
        {{ ref('match') }} m ON m.referee_id = referee.id
    JOIN
        {{ ref('competition') }} competition ON m.competition_id = competition.id
    WHERE
        referee.country = 'Germany'
    GROUP BY
        referee,
        league,
        season
)
SELECT
    referee,
{% for season in seasons %}{% for league in leagues %}
    sum(case when league = '{{ league }}' and season = '{{ season }}' then matches else 0 end) as "{{ league }} - {{ season }}",
{% endfor %}{% endfor %}
    sum(matches) as total_matches
FROM
    staging
