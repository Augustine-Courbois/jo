with 

source as (

    select * from {{ source('raw_data', 'democracy_index') }}

),

renamed as (

    select
        entity,
        year,
        `electoral democracy index` as electoral_democracy_index

    from source

)

select * from renamed
