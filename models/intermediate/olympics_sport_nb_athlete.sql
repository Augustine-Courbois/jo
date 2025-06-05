-- Agrégation au niveau de pays, year et sport_cleaned (pas sport_type)
-- Faire compte d'athlètes total, homme et femme à l'échelle du sport_cleaned
SELECT 
    code,
    country,
    region_wb, 
    year,
    code_year,
    city,
    sport_cleaned, 
    COUNT(DISTINCT name ) as nb_athletes,
    COUNT(DISTINCT CASE WHEN sex = 'M' THEN name END) as nb_men,
    COUNT(DISTINCT CASE WHEN sex = 'F' THEN name END) as nb_women
FROM {{ ref('olympics_enriched') }}
GROUP BY 
    country,
    code,
    region_wb,
    year,
    code_year,
    city,
    sport_cleaned
ORDER BY 
    year DESC, 
    country


    