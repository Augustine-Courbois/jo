with 
source as (
    select * 
    from {{ source('raw_data', 'democracy_index') }}
)
    
    select
        entity as country,
        year,
        `electoral democracy index` as electoral_democracy_index
    from source
    WHERE year >= 1990




