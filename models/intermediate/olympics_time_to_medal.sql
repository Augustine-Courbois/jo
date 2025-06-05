-- Aggrégation au niveau du pays
WITH time_medals AS (SELECT
    country
    , code
    , region_wb
    , sport_cleaned
    , count(distinct year) as nb_events
    , MIN(year) as first_participation
    , MIN(
        CASE 
            WHEN medal="Gold"
            OR medal="Silver"
            OR medal="Bronze"
            THEN year
            END
    ) as first_medal
    , MIN(
        CASE 
            WHEN medal="Gold"
            OR medal="Silver"
            OR medal="Bronze"
            THEN year
            END
    ) - MIN(year) as time_to_medal
    , MIN(
        CASE 
            WHEN medal="Gold"
            THEN year
            END
    ) as first_gold_medal
    , MIN(
        CASE 
            WHEN medal="Gold"
            THEN year
            END
    ) - MIN(year) as time_to_gold
FROM {{ ref('olympics_enriched') }}
GROUP BY country, code, region_wb, sport_cleaned), 

avg_time AS(
    SELECT 
        sport_cleaned
        , ROUND(AVG(time_to_medal),0) as avg_year_to_medals
        , ROUND(AVG(time_to_gold),0) as avg_year_to_gold
        , ROUND(AVG(nb_events),0) as avg_nb_events
    FROM time_medals
    GROUP BY sport_cleaned
), 

featured_sports AS(
    SELECT 
        sport_cleaned
        , count(distinct year) as nb_events
    FROM {{ ref('olympics_enriched') }}
    GROUP BY sport_cleaned
)

SELECT
    sport_cleaned
    , nb_events
    , avg_year_to_medals
    , avg_year_to_gold
FROM avg_time
LEFT JOIN featured_sports
USING(sport_cleaned)
order by sport_cleaned