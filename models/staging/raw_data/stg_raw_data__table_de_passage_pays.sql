with 

source as (

    select * from {{ source('raw_data', 'table_de_passage_pays') }}

),

renamed as (

    select
        string_field_0 as noc,
        string_field_1 as country,
        string_field_2 as code,
        string_field_3 as region_wb

    from source

)

select * from renamed
