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
    {{ dbt_utils.generate_surrogate_key(['retailer', 'last_updated']) }} as surrogate_key
from {{ ref('stg_petro') }}

{% if is_incremental() %}
where last_updated > (select max(last_updated) from {{ this }})
{% endif %}