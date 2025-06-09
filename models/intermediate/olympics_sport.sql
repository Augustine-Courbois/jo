--Matérialisation en table plutôt qu'une view --
{{ config(materialized='table') }}

--Aggrégation à l'échelle du sport--
WITH agg AS (
    SELECT
o.country, 
o.code,
o.region,
o.year, 
o.code_year,
o.city, 
o.sport_cleaned, 
o.code_year_sport_cleaned,
o.code_sport_cleaned,
SUM(a.nb_athletes) as nb_athletes, 
SUM(a.nb_athletes_men) as nb_athletes_men,
SUM(a.nb_athletes_women) as nb_athletes_women,
SUM(o.gold) as gold,
SUM(o.silver) as silver,
SUM(o.bronze) as bronze,
SUM(o.medals) as medals,
AVG(o.gdp) as gdp, 
AVG(o.gdp_per_capita) as gdp_per_capita,
o.income_category,
AVG(o.population) as population,
o.population_category,
AVG(o.human_development_index) as human_development_index,
AVG(o.electoral_democracy_index) as electoral_democracy_index,
AVG(o.gender_inequality_index) as gender_inequality_index,
AVG(o.prevalence_obesity_adults) as prevalence_obesity_adults,
AVG(o.is_host) as is_host,
AVG(o.nb_participations) as nb_participations,
AVG(t.nb_sport_olympics) as nb_sport_olympics,
AVG(t.avg_year_to_medals) as avg_year_to_medals,
AVG(t.avg_year_to_gold) as avg_year_to_gold,
AVG(m.nb_participation_sport_country) as nb_participation_sport_country,
AVG(m.first_participation) as first_participation,
AVG(m.first_medal) as first_medal,
AVG(m.time_to_medal) as time_to_medal,
AVG(m.first_gold_medal) as first_gold_medal,
AVG(m.time_to_gold) as time_to_gold,
AVG(b.gold_rate) as gold_rate,
AVG(b.medal_rate) as medal_rate
FROM {{ ref('olympics_genre') }} as o

LEFT JOIN {{ ref('olympics_sport_nb_athlete') }} as a
ON o.code_year_sport_cleaned = a.code_year_sport_cleaned

LEFT JOIN {{ ref('olympics_time_medals_country') }} as m
ON o.code_sport_cleaned = m.code_sport_cleaned

LEFT JOIN {{ ref('olympics_time_to_medal') }} as t
ON o.sport_cleaned = t.sport_cleaned

LEFT JOIN {{ ref('mart_niche_sports_medals') }} as b
ON o.sport_cleaned = b.sport_cleaned

GROUP BY 
o.country, 
o.code,
o.region,
o.year, 
o.code_year,
o.city, 
o.sport_cleaned, 
o.code_year_sport_cleaned,
o.code_sport_cleaned, 
o.income_category, 
o.population_category
)

SELECT 
  *,
  --% de médailles par sport sur le total des médailles sur l'année
  ROUND(SAFE_DIVIDE(medals, SUM(medals) OVER(PARTITION BY code_year)),6) as ratio_sport_medals_over_total_medals, 
  --% de sports médaillées / # de sports présentés
  ROUND(SAFE_DIVIDE(COUNTIF(medals>0) OVER(PARTITION BY code_year), COUNT(DISTINCT sport_cleaned) OVER(PARTITION BY code_year)),6) as ratio_medal_winning_sport, 
  --ratio d'athlètes envoyés sur médailles gagnées
  ROUND(SAFE_DIVIDE(SUM(medals) OVER(PARTITION BY year, sport_cleaned),sum(nb_athletes) OVER(PARTITION BY year, sport_cleaned)),6) as ratio_medals_over_athletes,
  --% d'athlètes par sport sur l'année pour un pays
  ROUND(SAFE_DIVIDE(nb_athletes, sum(nb_athletes) over (partition by year,country)),6) as ratio_perc_athletes_per_sport
FROM agg