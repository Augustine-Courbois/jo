--- Rajouter le pays et la région à partir de la base passage pays
--- Rajouter la nouvelle colonne de sports et la colonne sports Collective/Individuals/Mixte
SELECT 
    player_id,
        o.name,
        o.sex,
        o.team,
        o.noc,
        p.country,
        o.year,
        o.season,
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