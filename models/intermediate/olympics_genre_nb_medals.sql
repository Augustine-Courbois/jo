-- Aggrégation au pays, year, sport, sport_type
-- Subquery pour dédupliquer la base
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
-- Subquery 2
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
        -- Compte des médailles par sexe
        IF((medal = "Gold" OR medal = "Silver" OR medal = "Bronze") AND sex = "M", 1, 0) AS man,
        IF(medal = "Gold" AND sex = "M", 1, 0) AS gold_medal_m,
        IF(medal = "Silver" AND sex = "M", 1, 0) AS silver_medal_m,
        IF(medal = "Bronze" AND sex = "M", 1, 0) AS bronze_medal_m,
        IF((medal = "Gold" OR medal = "Silver" OR medal = "Bronze") AND sex = "F", 1, 0) AS woman,
        IF(medal = "Gold" AND sex = "F", 1, 0) AS gold_medal_w,
        IF(medal = "Silver" AND sex = "F", 1, 0) AS silver_medal_w,
        IF(medal = "Bronze" AND sex = "F", 1, 0) AS bronze_medal_w,
        IF(medal = "Gold", 1, 0) AS gold,
        IF(medal = "Silver", 1, 0) AS silver,
        IF(medal = "Bronze", 1, 0) AS bronze
    FROM deduplicate
)
-- Calcul du nombre de médailles basé sur la base dédupliquée et les colonnes créées
, medal_count AS (
SELECT
    country,
    code,
    region_wb,
    year,
    code_year,
    city,
    sport_cleaned,
    sport_type,
    CONCAT(code,"_",year,"_",sport,"_",sport_type) as code_year_sport_sport_type, -- pour les join avec base olympics_genre_nb_metals
    SUM(man) AS medals_man,
    SUM(gold_medal_m) AS gold_medal_m,
    SUM(silver_medal_m) AS silver_medal_m,
    SUM(bronze_medal_m) AS bronze_medal_m,
    SUM(woman) AS medals_woman,
    SUM(gold_medal_w) AS gold_medal_w,
    SUM(silver_medal_w) AS silver_medal_w,
    SUM(bronze_medal_w) AS bronze_medal_w,
    -- Médailles d'or
    CASE
        WHEN sport_type = "Mixed" THEN COALESCE(
            SAFE_DIVIDE(SUM(gold), NULLIF(SUM(gold_medal_m + gold_medal_w), 0)), 0)
        ELSE SUM(gold)
    END AS gold,
    -- Médailles d'argent
    CASE
        WHEN sport_type = "Mixed" THEN COALESCE(
            SAFE_DIVIDE(SUM(silver), NULLIF(SUM(silver_medal_m + silver_medal_w), 0)), 0)
        ELSE SUM(silver)
    END AS silver,
    -- Médailles de bronze
    CASE
        WHEN sport_type = "Mixed" THEN COALESCE(
            SAFE_DIVIDE(SUM(bronze), NULLIF(SUM(bronze_medal_m + bronze_medal_w), 0)), 0)
        ELSE SUM(bronze)
    END AS bronze,
    -- Total des médailles
    CASE
        WHEN sport_type = "Mixed" THEN (
            COALESCE(SAFE_DIVIDE(SUM(gold), NULLIF(SUM(gold_medal_m + gold_medal_w), 0)), 0) +
            COALESCE(SAFE_DIVIDE(SUM(silver), NULLIF(SUM(silver_medal_m + silver_medal_w), 0)), 0) +
            COALESCE(SAFE_DIVIDE(SUM(bronze), NULLIF(SUM(bronze_medal_m + bronze_medal_w), 0)), 0)
        )
        ELSE SUM(gold) + SUM(silver) + SUM(bronze)
    END AS medals
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
    sport_type,
    CONCAT(code,"_",year,"_",sport,"_",sport_type)
ORDER BY
    year DESC,
    country

)    

SELECT
country,
    code,
    region_wb,
    year,
    code_year,
    city,
    sport_cleaned,
    sport_type,
    code_year_sport_sport_type,  
    medals_man, 
    gold_medal_m,
    silver_medal_m,
    bronze_medal_m,
    medals_woman,
    gold_medal_w,
    silver_medal_w,
    bronze_medal_w,
    CAST(gold AS INT64) as gold,
    CAST(silver AS INT64) as  silver,
    CAST(bronze AS INT64) as bronze,
    CAST(medals AS INT64) as medals
FROM medal_count
