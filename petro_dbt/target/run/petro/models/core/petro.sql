
        
            delete from "petro"."main"."petro"
            where (
                surrogate_key) in (
                select (surrogate_key)
                from "petro__dbt_tmp20240621210037106502"
            );

        
    

    insert into "petro"."main"."petro" ("retailer", "brand", "address", "postcode", "latitude", "longitude", "b7", "e10", "e5", "sdv", "last_updated", "surrogate_key")
    (
        select "retailer", "brand", "address", "postcode", "latitude", "longitude", "b7", "e10", "e5", "sdv", "last_updated", "surrogate_key"
        from "petro__dbt_tmp20240621210037106502"
    )
  