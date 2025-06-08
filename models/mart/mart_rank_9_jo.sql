WITH sub AS (
  SELECT
    country,
    SUM(medals) AS total_medals_9_jo
  FROM {{ ref('olympics_country') }}
  -- on ajoute un filtre d'année si nécessaire, ex: WHERE year >= 1988
  GROUP BY country
)

SELECT 
  country,
  total_medals_9_jo,
  RANK() OVER (ORDER BY total_medals_9_jo DESC) AS rk_total_medals_9_jo
FROM sub