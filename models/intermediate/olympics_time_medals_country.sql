SELECT
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
GROUP BY country, code, region_wb, sport_cleaned