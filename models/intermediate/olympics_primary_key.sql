--- Rajouter la clé primaire (sport et event)
SELECT 
    *,
    CONCAT(sport,event) as primary_key
FROM {{ ref('stg_raw_data__olympic_raw') }}