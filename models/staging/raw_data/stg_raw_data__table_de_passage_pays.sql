with 

source as (

    select * from {{ source('raw_data', 'table_de_passage_pays') }}

),

renamed as (

    select
        noc,
        country,
        code_wb,
        code_wd,
        region_wb

    from source

)

select * from renamed
