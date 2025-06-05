--Matérialisation en table plutôt qu'une view --
{{ config(materialized='table') }}

--Aggrégation à l'échelle des pays--

WITH country_sub AS (
SELECT
o.country, 
o.code,
o.region,
o.year,
o.code_year,
o.city,
SUM(o.nb_participants) as nb_participants,
SUM(o.nb_participants_women) as nb_participants_women, 
SUM(o.nb_participants_men) as nb_participants_men, 
SUM(a.nb_athletes) as nb_athletes,
SUM(a.nb_athletes_men) as nb_athletes_men,
SUM(a.nb_athletes_women) as nb_athletes_women,
SUM(o.gold) as gold,
SUM(o.silver) as silver,
SUM(o.bronze) as bronze,
SUM(o.medals) as medals,
SUM(o.gold_medal_m) as gold_medal_m,
SUM(o.silver_medal_m) as silver_medal_m,
SUM(o.bronze_medal_m) as bronze_medal_m,
SUM(o.medals_man) as medals_man,
SUM(o.gold_medal_w) as gold_medal_w,
SUM(o.silver_medal_w) as silver_medal_w,
SUM(o.bronze_medal_w) as bronze_medal_w,
SUM(o.medals_woman) as medals_woman, 
AVG(o.gdp) as gdp, 
AVG(o.gdp_per_capita) as gdp_per_capita,
o.income_category,
AVG(o.population) as population,
o.population_category,
AVG(o.human_development_index) as human_development_index,
AVG(o.electoral_democracy_index) as electoral_democracy_index,
AVG(o.gender_inequality_index) as gender_inequality_index,
AVG(o.prevalence_obesity_adults) as prevalence_obesity_adults,
AVG(o.nb_participations) as nb_participations,
AVG(o.is_host) as is_host,
AVG(b.first_participation_jo) as first_participation_jo
FROM {{ ref('olympics_genre') }} as o

LEFT JOIN {{ ref('olympics_sport_nb_athlete') }} as a
USING (code_year_sport_cleaned)

-- join la première année de participation d'un pays aux JO
LEFT JOIN {{ ref('first_participation_jo') }} as b
ON b.code = o.code

GROUP BY 
o.country, 
o.code,
o.region,
o.year,
o.code_year,
o.city,
o.income_category,
o.population_category

)

--- Rajout des ratios medals/PIB

SELECT 
country, 
code,
region,
year,
code_year,
city,
nb_participants,
nb_participants_women, 
nb_participants_men, 
nb_athletes,
nb_athletes_men,
nb_athletes_women,
gold,
silver,
bronze,
medals,
gold_medal_m,
silver_medal_m,
bronze_medal_m,
medals_man,
gold_medal_w,
silver_medal_w,
bronze_medal_w,
medals_woman, 
gdp, 
SAFE_DIVIDE(medals, gdp) as ratio_medals_gdp,
gdp_per_capita,
SAFE_DIVIDE(medals, gdp_per_capita) as ratio_medals_gdp_per_capita,
income_category,
population,
SAFE_DIVIDE(medals, population) as ratio_medals_population,
population_category,
human_development_index,
SAFE_DIVIDE(medals, human_development_index) as ratio_medals_human_development_index,
electoral_democracy_index,
SAFE_DIVIDE(medals, electoral_democracy_index) as ratio_medals_electoral_democracy_index,
gender_inequality_index,
SAFE_DIVIDE(medals, gender_inequality_index) as ratio_medals_gender_inequality_index,
SAFE_DIVIDE(medals_woman, gender_inequality_index) as ratio_medals_woman_gender_inequality_index,
prevalence_obesity_adults,
SAFE_DIVIDE(medals, prevalence_obesity_adults) as ratio_medals_prevalence_obesity_adults,
nb_participations,
is_host, 
first_participation_jo
FROM country_sub

