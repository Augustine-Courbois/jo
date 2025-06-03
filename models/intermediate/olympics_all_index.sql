SELECT
e.*,
g.gdp,
c.gdp_per_capita,
c.income_category,
p.population,
h.human_development_index,
d.electoral_democracy_index,
i.gender_inequality_index,
o.prevalence_obesity_adults
FROM {{ ref('olympics_enriched') }} as e
JOIN {{ ref('stg_raw_data__gdp') }} as g
USING (code_year)
JOIN {{ ref('stg_raw_data__gdp_per_capita') }} as c
USING (code_year)
JOIN {{ ref('stg_raw_data__population') }} as p
USING (code_year)
JOIN {{ ref('stg_raw_data__hdi') }} as h
USING (code_year)
JOIN {{ ref('democracy_index') }} as d
USING (code_year)
JOIN {{ ref('stg_raw_data__gender_inequality_index') }} as i
USING (code_year)
JOIN {{ ref('stg_raw_data__obesity') }} as o
USING (code_year)
