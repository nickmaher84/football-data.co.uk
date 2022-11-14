{{ config(indexes=[{'columns': ['last_modified']}], unique_key='id') }}

WITH
staging AS (
    SELECT DISTINCT
        source,
        referee                                     as referee_name,
        CASE
            WHEN referee LIKE '%,%'
            THEN concat_ws(' '
                 , replace(trim(split_part(referee, ',', 2)), '.', '')
                 , split_part(referee, ',', 1)
                 )
            ELSE replace(referee, '.', '')
        END                                         as full_name,
        COALESCE(stg.country, country.country_name) as country,
        season.last_modified
    FROM
        {{ ref('stg_match') }} stg
    JOIN
        {{ source('football-data', 'season') }} season USING (url)
    JOIN
        {{ source('football-data', 'league') }} league USING (division)
    JOIN
        {{ source('football-data', 'country') }} country USING (country_code)
    WHERE
        referee IS NOT NULL
)
SELECT
    {{ dbt_utils.surrogate_key(
        ['source', 'referee_name', 'country']
    ) }}                                                       as id,
    referee_name                                               as name,
    full_name                                                  as full_name,
    trim(
        substring(full_name from '^([A-Za-zäéöüß-]*[A-Z ]*)[A-Z]')
    )                                                          as first_name,
    trim(
        substring(full_name from '.(([A-Z][a-zäöüß()'']+ ?)+)$')
    )                                                          as last_name,
    country                                                    as country,
    count(*)                                                   as matches,
    max(last_modified)                                         as last_modified
FROM
    staging
{% if is_incremental() %}
WHERE
    last_modified > (select max(this.last_modified) from {{ this }} this)
{% endif %}
GROUP BY
    source,
    referee_name,
    full_name,
    country