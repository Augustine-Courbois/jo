--- Rajouter le pays et la région à partir de la base passage pays
--- Rajouter la nouvelle colonne de sports et la colonne sports Collective/Individuals/Mixte
SELECT 
        o.player_id,
        o.name,
        o.sex,
        o.noc,
        p.country,
        p.region_wb, 
        o.year,
        o.city,
        o.sport,
        o.event,
        s.sport_cleaned,
        s.sport_type,
        o.medal, 
FROM {{ ref('olympics_primary_key') }} as o
JOIN {{ ref('stg_raw_data__table_de_passage_pays') }} as p
USING (noc)
JOIN {{ ref('stg_raw_data__table_de_passage_sports') }} as s
USING (primary_key)