SELECT
  *,
  CASE
    WHEN population < 500000 THEN 'Micro state'
    WHEN population < 5000000 THEN 'Small country'
    WHEN population < 50000000 THEN 'Medium-sized country'
    WHEN population < 200000000 THEN 'Large country'
    ELSE 'Very large country'
  END AS population_category
FROM {{ ref('olympics_all_index') }}