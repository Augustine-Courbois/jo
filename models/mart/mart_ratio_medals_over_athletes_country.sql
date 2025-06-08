--Calcul du ratio_medals_over_athletes_country et de son rang
SELECT
    country,
    year,
    SAFE_DIVIDE(medals,nb_athletes) as ratio_medals_over_athletes_country, 
    RANK() OVER (PARTITION BY year ORDER BY SAFE_DIVIDE(medals,nb_athletes) DESC) AS rk_ratio_medals_over_athletes_country
FROM {{ ref('olympics_country') }}
ORDER BY year desC, rk_ratio_medals_over_athletes_country ASC
