-- Rajout de la primary_key construit comme code_year
-- Compter le nb d'engagés, hommes et femmes
SELECT
    country,
    code, -- à remplacer par code
    region_wb as region, 
    year,
    code_year,
    city,
    sport_cleaned as sport,
    sport_type, 
    COUNT(DISTINCT name) as nb_athletes,
    COUNT(DISTINCT CASE WHEN sex = 'F' THEN name END) as nb_women,
    COUNT(DISTINCT CASE WHEN sex = 'M' THEN name END) as nb_men,
FROM {{ ref('olympics_enriched') }}
GROUP BY 
    country,
    code, -- à remplacer par code
    region_wb, 
    year,
    code_year,
    city,
    sport_cleaned,
    sport_type