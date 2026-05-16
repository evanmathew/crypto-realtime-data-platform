
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select ingested_at
from CRYPTO_DB.STAGING.stg_crypto_prices
where ingested_at is null



  
  
      
    ) dbt_internal_test