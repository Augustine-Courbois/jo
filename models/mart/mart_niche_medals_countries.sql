SELECT 
  sport_cleaned
  , year
  , country
  , COUNT(DISTINCT IF(medal="Gold",CONCAT(year,event),NULL)) as nb_of_gold
FROM {{ ref('olympics_enriched') }}
WHERE year >= 1992 AND medal="Gold"
GROUP BY sport_cleaned, medal, country, year
order by sport_cleaned, year, IF(medal="Gold",country,NULL) desc