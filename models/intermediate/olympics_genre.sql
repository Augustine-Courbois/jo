SELECT 
p.country, 
p.code,
p.region,
p.year,
p.code_year,
p.city,
p.sport,
p.sport_type,
p.code_year_sport_sport_type,
p.nb_participants,
p.nb_women, 
p.nb_men,
m.gold,
m.silver,
m.bronze,
m.medals,
m.gold_medal_m,
m.silver_medal_m,
m.bronze_medal_m,
m.medals_man,
m.gold_medal_w,
m.silver_medal_w,
m.bronze_medal_w,
m.medals_woman, 
i.gdp, 
i.gdp_per_capita,
i.income_category,
i.population,
i.population_category,
i.human_development_index,
i.electoral_democracy_index,
i.gender_inequality_index,
i.prevalence_obesity_adults,
n.nb_participations,
t.nb_events,
t.avg_year_to_medals,
t.avg_year_to_gold
FROM {{ ref('olympics_genre_nb_participants') }} as p
LEFT JOIN {{ ref('olympics_genre_nb_medals') }} as m
USING (code_year_sport_sport_type)

LEFT JOIN {{ ref('olympics_genre_all_index') }} as i
ON p.code_year = i.code_year

LEFT JOIN {{ ref('olympics_country_participation_jo') }} as n
ON p.code = n.code

LEFT JOIN {{ ref('olympics_time_to_medal') }} as t
ON p.sport = t.sport_cleaned
WHERE p.year > 1991