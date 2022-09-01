SELECT
    *
FROM
    {{ ref('competition') }} c
WHERE
    country = 'Germany'
