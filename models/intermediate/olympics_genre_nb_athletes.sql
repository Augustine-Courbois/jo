/*-- Subquery pour dédupliquer la base 
WITH deduplicate AS (
    SELECT DISTINCT 
        country,
        code, 
        region_wb,
        year,
        code_year,
        city,
        sport,
        event, 
        sport_cleaned,
        sport_type,
        medal, 
        sex
    FROM {{ ref('olympics_enriched') }}
)

-- Subquery 2 --  
, count AS (
    SELECT 
        country,
        code,
        region_wb,
        year,
        code_year,
        city,
        sport,
        event,
        sport_cleaned,
        sport_type,
        -- Compte des athlètes par sexe
        IF(sex = "M", 1, 0) AS man,
        IF(sex = "F", 1, 0) AS woman,
    FROM deduplicate
)

-- Calcul du nombre de médailles basé sur la base dédupliquée et les colonnes créées
SELECT
    country,
    code,
    region_wb,
    year,
    code_year,
    city,
    sport_cleaned,
    sport_type,
    SUM(man) AS nb_men,
    SUM(woman) AS nb_women,
    SUM(man)+SUM(woman) AS nb_athletes,
FROM count
--WHERE year >= 1992
GROUP BY 
    country,
    code,
    region_wb,
    year,
    code_year,
    city,
    sport_cleaned,
    sport_type
ORDER BY 
    year DESC, 
    country
    */
