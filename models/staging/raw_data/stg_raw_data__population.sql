with 

source as (

    select * from {{ source('raw_data', 'population') }}

),

renamed as (

    select
        string_field_0,
        string_field_1,
        string_field_2,
        string_field_3,
        int64_field_4,
        int64_field_5,
        int64_field_6,
        int64_field_7,
        int64_field_8,
        int64_field_9,
        int64_field_10,
        int64_field_11,
        int64_field_12,
        int64_field_13,
        int64_field_14,
        int64_field_15,
        int64_field_16,
        int64_field_17,
        int64_field_18,
        int64_field_19,
        int64_field_20,
        int64_field_21,
        int64_field_22,
        int64_field_23,
        int64_field_24,
        int64_field_25,
        int64_field_26,
        int64_field_27,
        int64_field_28,
        int64_field_29,
        int64_field_30,
        int64_field_31,
        int64_field_32,
        int64_field_33,
        int64_field_34,
        int64_field_35,
        double_field_36,
        int64_field_37,
        double_field_38,
        int64_field_39,
        double_field_40,
        double_field_41,
        double_field_42,
        int64_field_43,
        double_field_44,
        double_field_45,
        int64_field_46,
        int64_field_47,
        double_field_48,
        double_field_49,
        int64_field_50,
        double_field_51,
        int64_field_52,
        int64_field_53,
        int64_field_54,
        double_field_55,
        int64_field_56,
        int64_field_57,
        double_field_58,
        double_field_59,
        double_field_60,
        double_field_61,
        double_field_62,
        int64_field_63,
        double_field_64,
        int64_field_65,
        double_field_66,
        int64_field_67,
        string_field_68,
        string_field_69

    from source

)

select * from renamed
