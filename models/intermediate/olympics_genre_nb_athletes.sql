-- Rajout de la primary_key construit comme code_year
-- Compter le nb d'athlètes, athlètes femmes, athlètes homme
SELECT
    country,
    noc,
    region_wb as region, 
    year,
    CONCAT(noc,"_",year)as code_year,
    city,
    sport_cleaned as sport,
    COUNT(DISTINCT name) as nb_athletes,
    COUNT(DISTINCT CASE WHEN sex = 'F' THEN name END) as nb_women,
    COUNT(DISTINCT CASE WHEN sex = 'M' THEN name END) as nb_men,
FROM {{ ref('olympics_enriched') }}
GROUP BY 
    country,
    noc,
    region_wb, 
    year,
    CONCAT(noc,"_",year),
    city,
    sport_cleaned