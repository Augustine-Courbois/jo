/* SELECT
  sport_cleaned
  , COUNT(DISTINCT IF(medal="Gold",country,NULL)) as nb_of_countries_gold
  , COUNT(DISTINCT IF(medal="Gold",CONCAT(year,event),NULL)) as nb_of_gold
  , COUNT(DISTINCT IF(medal="Gold" OR medal="Silver" OR medal="Bronze",country,NULL)) as nb_of_countries_medals
FROM {{ ref('olympics_enriched') }}
WHERE year >= 1992
GROUP BY sport_cleaned
order by COUNT(DISTINCT IF(medal="Gold",country,NULL)) desc
;
SELECT 
  sport_cleaned
  , year
  , IF(medal="Gold",country,NULL) as gold
  , IF(medal="Silver",country,NULL) as silver
  , IF(medal="Bronze",country,NULL) as bronze
FROM {{ ref('olympics_enriched') }}
WHERE year >= 1992 AND (medal="Gold" or medal="Silver" or medal="Bronze")
GROUP BY sport_cleaned, medal, country, year
order by sport_cleaned, year, IF(medal="Gold",country,NULL) desc
*/