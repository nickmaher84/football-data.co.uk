SELECT DISTINCT
  row.header,
  raw.url
FROM
  {{ source('football-data', 'raw') }} raw,
  json_each_text(raw.json) row(header, value)
WHERE
  raw.json ->> 'Date' != ''
ORDER BY
  row.header,
  raw.url