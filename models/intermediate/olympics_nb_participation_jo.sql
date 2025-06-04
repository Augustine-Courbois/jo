SELECT
  code AS country_code,
  COUNT(DISTINCT year) AS nb_participations
FROM {{ ref('olympics_enriched') }}
GROUP BY code
ORDER BY nb_participations DESC

