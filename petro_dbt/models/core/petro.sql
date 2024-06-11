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
            {{ dbt_utils.generate_surrogate_key(['retailer', 'brand', 'address', 'last_updated']) }} as surrogate_key
        from {{ ref('stg_petro') }}

        {% if is_incremental() %}
        where last_updated > (select max(last_updated) from {{ this }})
        {% endif %}

)
select *
from petro_data