with source as (
    select * from {{ source('raw_data', 'gdp') }}

),

renamed as (
    select
        string_field_0,
        string_field_1,
        string_field_2,
        string_field_3,
        double_field_4,
        double_field_5,
        double_field_6,
        double_field_7,
        double_field_8,
        double_field_9,
        double_field_10,
        double_field_11,
        double_field_12,
        double_field_13,
        double_field_14,
        double_field_15,
        double_field_16,
        double_field_17,
        double_field_18,
        double_field_19,
        double_field_20,
        double_field_21,
        double_field_22,
        double_field_23,
        double_field_24,
        double_field_25,
        double_field_26,
        double_field_27,
        double_field_28,
        double_field_29,
        double_field_30,
        double_field_31,
        double_field_32,
        double_field_33,
        double_field_34,
        double_field_35,
        double_field_36,
        double_field_37,
        double_field_38,
        double_field_39,
        double_field_40,
        double_field_41,
        double_field_42,
        double_field_43,
        double_field_44,
        double_field_45,
        double_field_46,
        double_field_47,
        double_field_48,
        double_field_49,
        double_field_50,
        double_field_51,
        double_field_52,
        double_field_53,
        double_field_54,
        double_field_55,
        double_field_56,
        double_field_57,
        double_field_58,
        double_field_59,
        double_field_60,
        double_field_61,
        double_field_62,
        double_field_63,
        double_field_64,
        double_field_65,
        double_field_66,
        double_field_67,
        int64_field_68,
        string_field_69

    from source

)

--Step1: supprimer les colonnes indésirables--
, supp AS (
SELECT *
EXCEPT
(string_field_2, 
string_field_3, 
double_field_4,
double_field_5,
double_field_6,
double_field_7,
double_field_8,
double_field_9,
double_field_10,
double_field_11,
double_field_12,
double_field_13,
double_field_14,
double_field_15,
double_field_16,
double_field_17,
double_field_18,
double_field_19,
double_field_20,
double_field_21,
double_field_22,
double_field_23,
double_field_24,
double_field_25,
double_field_26,
double_field_27,
double_field_28,
double_field_29,
double_field_30,
double_field_31,
double_field_32,
double_field_33,
int64_field_68,
string_field_69)
FROM renamed
)

---renommer les colonnes--
, rename AS (
SELECT 
string_field_0 as country,
string_field_1 as code,
double_field_34 as `1990`,
double_field_35 as `1991`,
double_field_36 as `1992`,
double_field_37 as `1993`,
double_field_38 as `1994`,
double_field_39 as `1995`,
double_field_40 as `1996`,
double_field_41 as `1997`,
double_field_42 as `1998`,
double_field_43 as `1999`,
double_field_44 as `2000`,
double_field_45 as `2001`,
double_field_46 as `2002`,
double_field_47 as `2003`,
double_field_48 as `2004`,
double_field_49 as `2005`,
double_field_50 as `2006`,
double_field_51 as `2007`,
double_field_52 as `2008`,
double_field_53 as `2009`,
double_field_54 as `2010`,
double_field_55 as `2011`,
double_field_56 as `2012`,
double_field_57 as `2013`,
double_field_58 as `2014`,
double_field_59 as `2015`,
double_field_60 as `2016`,
double_field_61 as `2017`,
double_field_62 as `2018`,
double_field_63 as `2019`,
double_field_64 as `2020`,
double_field_65 as `2021`,
double_field_66 as `2022`,
double_field_67 as `2023`
FROM supp
)

--Step3: passage des colonnes en lignes--
, pivot AS (
SELECT 
  country,
  code,
  CAST(annee_col AS INT64) AS year,
  gdp
FROM rename
UNPIVOT (
  gdp FOR annee_col IN (
    `1990`,
`1991`,
`1992`,
`1993`,
`1994`,
`1995`,
`1996`,
`1997`,
`1998`,
`1999`,
`2000`,
`2001`,
`2002`,
`2003`,
`2004`,
`2005`,
`2006`,
`2007`,
`2008`,
`2009`,
`2010`,
`2011`,
`2012`,
`2013`,
`2014`,
`2015`,
`2016`,
`2017`,
`2018`,
`2019`,
`2020`,
`2021`,
`2022`,
`2023`
  )
)
)
-- remplacer les index 2023 en 2024 car nous n'avons pas encore accès aux données 2024 --
, date_change AS (
SELECT
country, 
code, 
CASE 
      WHEN year = 2023 THEN 2024
      ELSE year
END AS year,
gdp
FROM pivot
)

--créer les code_year--
SELECT
CONCAT(code,"_",CAST(year AS STRING)) as code_year, 
country, 
code, 
year,
gdp
from date_change