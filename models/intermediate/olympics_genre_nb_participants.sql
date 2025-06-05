-- Aggrégation au pays, year, sport, sport_type
-- Compter le nb d'engagés, d'engagés hommes et d'engagés femmes
-- Ajout d'une clé unique code_year_sport_sport_type
SELECT
    country,
    code,
    region_wb as region, 
    year,
    code_year,
    city,
    sport_cleaned as sport,
    sport_type, 
    CONCAT(code,"_",year,"_",sport,"_",sport_type) as code_year_sport_sport_type, -- pour les join avec base olympics_genre_nb_metals 
    COUNT(DISTINCT name) as nb_participants,
    COUNT(DISTINCT CASE WHEN sex = 'F' THEN name END) as nb_women,
    COUNT(DISTINCT CASE WHEN sex = 'M' THEN name END) as nb_men,
FROM {{ ref('olympics_enriched') }}
GROUP BY 
    country,
    code,
    region_wb, 
    year,
    code_year,
    city,
    sport_cleaned,
    sport_type, 
    code_year_sport_sport_type

    