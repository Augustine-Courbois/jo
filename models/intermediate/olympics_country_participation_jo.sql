-- Compter le nb de participations d'un pays aux JO
SELECT
  code as code,
  COUNT(DISTINCT year) AS nb_participations
FROM {{ ref('olympics_enriched') }}
GROUP BY code