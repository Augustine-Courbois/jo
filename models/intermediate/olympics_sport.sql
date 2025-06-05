SELECT
o.country, 
o.code,
o.region,
o.year, 
o.code_year,
o.city, 
o.sport_cleaned, 
o.gold,
o.silver,
o.bronze,
o.medals,
o.gdp, 
o.gdp_per_capita,
o.income_category,
o.population,
o.population_category,
o.human_development_index,
o.electoral_democracy_index,
o.gender_inequality_index,
o.prevalence_obesity_adults,
o.is_host,
o.nb_participations,
o.nb_sport_olympics,
o.avg_year_to_medals,
o.avg_year_to_gold,
a.nb_athletes, 
a.nb_athletes_men,
a.nb_athletes_women,
m.nb_participation_sport_country,
m.first_participation,
m.first_medal,
m.time_to_medal,
m.first_gold_medal,
m.time_to_gold
FROM {{ ref('olympics_genre') }} as o

LEFT JOIN {{ ref('olympics_sport_nb_athlete') }} as a
ON o.code_year_sport_cleaned = a.code_year_sport_cleaned

LEFT JOIN {{ ref('olympics_time_medals_country') }} as m
ON o.code_sport_cleaned = m.code_sport_cleaned