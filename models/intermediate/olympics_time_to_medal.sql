WITH time_medals AS (SELECT
    noc
    , country
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
GROUP BY noc, country, region_wb, sport_cleaned)

SELECT 
    sport_cleaned
    , ROUND(AVG(time_to_medal),0) as avg_year_to_medals
    , ROUND(AVG(time_to_gold),0) as avg_year_to_gold
FROM time_medals
GROUP BY sport_cleaned
ORDER BY sport_cleaned