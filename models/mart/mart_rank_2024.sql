-- Calculer le rang du # de medals, medals/GDP, medals/GDP per capita et medals/pop pour une pays sur l'édition 2024
-- Utilité : ranker les pays entre eux sur 2024 

SELECT 
    country, 
    code, 
    year, 
    medals, 
    ratio_medals_gdp,
    ratio_medals_gdp_per_capita,
    ratio_medals_population,
    RANK() OVER (PARTITION BY year ORDER BY medals DESC) AS rk_medals_2024,
    RANK() OVER (PARTITION BY year ORDER BY ratio_medals_gdp DESC) AS rk_ratio_medals_gdp_2024,
    RANK() OVER (PARTITION BY year ORDER BY ratio_medals_gdp_per_capita DESC) AS rk_ratio_medals_gdp_per_capita_2024,
    RANK() OVER (PARTITION BY year ORDER BY ratio_medals_population DESC) AS rk_ratio_medals_population_2024
FROM {{ ref('olympics_country') }}
WHERE year = 2024