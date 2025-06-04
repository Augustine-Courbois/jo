WITH index AS(
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
LEFT JOIN {{ ref('stg_raw_data__gdp') }} as g
USING (code_year)
LEFT JOIN {{ ref('stg_raw_data__gdp_per_capita') }} as c
USING (code_year)
LEFT JOIN {{ ref('stg_raw_data__population') }} as p
USING (code_year)
LEFT JOIN {{ ref('stg_raw_data__hdi') }} as h
USING (code_year)
LEFT JOIN {{ ref('democracy_index') }} as d
USING (code_year)
LEFT JOIN {{ ref('stg_raw_data__gender_inequality_index') }} as i
USING (code_year)
LEFT JOIN {{ ref('stg_raw_data__obesity') }} as o
USING (code_year)
)

SELECT
    country,
    code, -- à remplacer par code
    region_wb, 
    year,
    code_year,
    city,
    sport_cleaned,
    sport_type, 
    SAFE_DIVIDE(gdp,1000000000) as gdp, --conversion du PIB en milliards de $ pour plus de lisibilité
    ROUND(gdp_per_capita,0) as gdp_per_capita, -- round à l'unité
    income_category,
    SAFE_DIVIDE(population,1000000) as population, -- conversion de la population en million d'habitants 
    human_development_index,
    electoral_democracy_index,
    gender_inequality_index,
    prevalence_obesity_adults,
    CASE
      WHEN population < 500000 THEN 'Micro state'
      WHEN population < 5000000 THEN 'Small country'
      WHEN population < 50000000 THEN 'Medium-sized country'
      WHEN population < 200000000 THEN 'Large country'
      ELSE 'Very large country'
    END AS population_category
FROM index

--Création des ratios par index--

--SELECT
--*
--medals--,
--SUM(medals) / gdp over(partition by code_year) as ratio_medals_gdp,
--SUM(medals) / gdp_per_capita over(partition by code_year) as ratio_medals_gdp_per_capita,
--SUM(medals) / population over(partition by code_year) as ratio_medals_gdp,
--SUM(medals) / human_development_index over(partition by code_year) as ratio_medals_human_development_index,
--SUM(medals) / electoral_democracy_index over(partition by code_year) as ratio_medals_electoral_democracy_index,
--SUM(medals) / gender_inequality_index over(partition by code_year) as ratio_medals_gender_inequality_index,
--SUM(medals) / prevalence_obesity_adults over(partition by code_year) as ratio_medals_prevalence_obesity_adults,