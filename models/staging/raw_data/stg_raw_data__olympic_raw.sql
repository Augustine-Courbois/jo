with 

source as (

    select * from {{ source('raw_data', 'olympic_raw') }}

),

renamed as (

    select
        player_id,
        name,
        sex,
        team,
        noc,
        year,
        season,
        city,
        sport,
        event,
        medal

    from source

)

select * from renamed
