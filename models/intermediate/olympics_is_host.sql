SELECT 
  gdp.code_year,
  gdp.country,
  gdp.year,
  CASE 
    WHEN (gdp.country = host.country AND gdp.year = host.year) THEN 1
    ELSE 0
  END AS is_host
FROM {{ ref('stg_raw_data__gdp_per_capita') }} gdp
LEFT JOIN (
  SELECT 'Spain' AS country, 1992 AS year UNION ALL
  SELECT 'United States', 1996 UNION ALL
  SELECT 'Australia', 2000 UNION ALL
  SELECT 'Greece', 2004 UNION ALL
  SELECT 'China', 2008 UNION ALL
  SELECT 'United Kingdom', 2012 UNION ALL
  SELECT 'Brazil', 2016 UNION ALL
  SELECT 'Japan', 2020 UNION ALL
  SELECT 'France', 2024
) AS host
ON gdp.country = host.country AND gdp.year = host.year
