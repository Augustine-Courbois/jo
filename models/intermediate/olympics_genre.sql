SELECT 
p.country, 
p.code,
p.region,
p.year,
p.code_year,
p.city,
p.sport_cleaned,
p.sport_type,
CONCAT(p.code_year,"_",p.sport_cleaned) as code_year_sport_cleaned,
CONCAT(p.code,"_",p.sport_cleaned) as code_sport_cleaned,
p.code_year_sport_cleaned_sport_type,
p.nb_participants,
p.nb_participants_women, 
p.nb_participants_men,
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
t.nb_sport_olympics,
t.avg_year_to_medals,
t.avg_year_to_gold,
h.is_host
FROM {{ ref('olympics_genre_nb_participants') }} as p
LEFT JOIN {{ ref('olympics_genre_nb_medals') }} as m
USING (code_year_sport_cleaned_sport_type)

LEFT JOIN {{ ref('olympics_genre_all_index') }} as i
USING (code_year_sport_cleaned_sport_type)

LEFT JOIN {{ ref('olympics_country_participation_jo') }} as n
ON p.code = n.code

LEFT JOIN {{ ref('olympics_time_to_medal') }} as t
ON p.sport_cleaned = t.sport_cleaned

LEFT JOIN {{ ref('olympics_is_host') }} as h
ON p.code_year = h.code_year

WHERE p.year > 1991