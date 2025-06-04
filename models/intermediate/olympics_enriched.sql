--- Subquery pr rajouter la clé primaire (sport et event) pour pouvoir importer de la table passage sport sport_cleaned et sport_type
WITH key_union AS (
    SELECT 
    *,
    CONCAT(sport,event) as primary_key
    FROM {{ ref('stg_raw_data__olympic_raw') }}
)

--- Rajouter le pays et la région à partir de la base passage pays
--- Rajouter la nouvelle colonne de sports et la colonne sports Collective/Individuals/Mixte
SELECT 
        o.player_id,
        o.name,
        o.sex,
        o.noc,
        p.code,
        p.country,
        p.region_wb, 
        o.year,
        CONCAT(p.code,"_",o.year) as code_year,
        o.city,
        o.sport,
        o.event,
        s.sport_cleaned,
        s.sport_type,
        o.medal, 
FROM key_union as o
JOIN {{ ref('stg_raw_data__table_de_passage_pays') }} as p
USING (noc)
JOIN {{ ref('stg_raw_data__table_de_passage_sports') }} as s
USING (primary_key)