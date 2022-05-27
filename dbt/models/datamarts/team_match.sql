{{
  config(
    materialized="view",
  )
}}
WITH
staging AS (
  SELECT
    id as match_id,
    competition_id,
    date,
    time,
    UNNEST(ARRAY[home_team_id,away_team_id])   as team_id,
    UNNEST(ARRAY[away_team_id,home_team_id])   as opponent_id,
    UNNEST(ARRAY['H','A'])                     as home_away,
    UNNEST(ARRAY[home_goals,away_goals])       as goals_for,
    UNNEST(ARRAY[away_goals,home_goals])       as goals_against,
    UNNEST(ARRAY[ht_home_goals,ht_away_goals]) as ht_goals_for,
    UNNEST(ARRAY[ht_away_goals,ht_home_goals]) as ht_goals_against,
    referee,
    attendance
  FROM
    {{ ref('match') }} m
)
SELECT
  match_id,
  competition_id,
  date,
  time,
  team_id,
  opponent_id,
  home_away,
  goals_for,
  goals_against,
  CASE
    WHEN goals_for > goals_against
    THEN 'W'
    WHEN goals_for = goals_against
    THEN 'D'
    WHEN goals_for < goals_against
    THEN 'L'
  END              as result,
  ht_goals_for,
  ht_goals_against,
  CASE
    WHEN ht_goals_for > ht_goals_against
    THEN 'W'
    WHEN ht_goals_for = ht_goals_against
    THEN 'D'
    WHEN ht_goals_for < ht_goals_against
    THEN 'L'
  END              as ht_result,
  referee,
  attendance
FROM
  staging