-- calcul de la première année de participation aux JO
SELECT
    code,
    ROUND(MIN(year),0) AS first_participation_jo
FROM {{ ref('olympics_enriched') }}
GROUP BY code