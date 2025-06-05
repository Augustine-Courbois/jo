--Matérialisation en table plutôt qu'une view --
{{ config(materialized='table') }}

--Aggrégation à l'échelle du sport--
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
AVG(m.time_to_gold) as time_to_gold

FROM {{ ref('olympics_genre') }} as o

LEFT JOIN {{ ref('olympics_sport_nb_athlete') }} as a
ON o.code_year_sport_cleaned = a.code_year_sport_cleaned

LEFT JOIN {{ ref('olympics_time_medals_country') }} as m
ON o.code_sport_cleaned = m.code_sport_cleaned

LEFT JOIN {{ ref('olympics_time_to_medal') }} as t
ON o.sport_cleaned = t.sport_cleaned

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