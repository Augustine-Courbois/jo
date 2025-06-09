SELECT
  sport_cleaned
  , COUNT(DISTINCT IF(medal="Gold",country,NULL)) as nb_of_countries_gold
  , COUNT(DISTINCT IF(medal="Gold",CONCAT(year,event),NULL)) as nb_of_olympic_contests
  , COUNT(DISTINCT IF(medal="Gold" OR medal="Silver" OR medal="Bronze",country,NULL)) as nb_of_countries_medals
  , COUNT(DISTINCT IF(medal<>"No medal",CONCAT(year,event,medal),NULL)) as nb_of_medals
  , COUNT(DISTINCT IF(medal="Gold",year,NULL)) as nb_of_olympics
  , ROUND(SAFE_DIVIDE(COUNT(DISTINCT IF(medal="Gold",country,NULL)),COUNT(DISTINCT IF(medal="Gold",CONCAT(year,event),NULL))),2) as gold_rate
  , ROUND(SAFE_DIVIDE(COUNT(DISTINCT IF(medal="Gold" OR medal="Silver" OR medal="Bronze",country,NULL)),COUNT(DISTINCT IF(medal<>"No medal",CONCAT(year,event,medal),NULL))),2) as medal_rate
FROM {{ ref('olympics_enriched') }}
WHERE year >= 1992
GROUP BY sport_cleaned
order by COUNT(DISTINCT IF(medal="Gold",country,NULL)) desc