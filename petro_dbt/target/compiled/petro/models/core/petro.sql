with petro_data as (

        select 
            retailer,
            brand,
            address,
            postcode,
            location_latitude as latitude,
            location_longitude as longitude,
            "prices_B7" as b7,
            "prices_E10" as e10,
            "prices_E5" as e5,
            "prices_SDV" as sdv,
            last_updated,
            -- row_number() over(partition by retailer, brand, strftime(DATE last_updated, '%d/%m/%Y'), address, postcode order by last_updated desc) as row_num,
            md5(cast(coalesce(cast(retailer as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(brand as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(address as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(last_updated as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as surrogate_key
        from "petro"."main"."stg_petro"

        

)
select *
from petro_data