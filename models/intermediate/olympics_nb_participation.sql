SELECT
  nc.region AS country_name,
  COUNT(DISTINCT oraw.year) AS nb_participations
FROM
  {{ ref('stg_raw_data__olympic_raw') }} AS oraw
JOIN
  {{ ref('stg_raw_data__noc_codes') }} AS nc
  ON oraw.noc = nc.noc
WHERE
  oraw.season = 'Summer'
GROUP BY
  country_name
ORDER BY
  nb_participations DESC
