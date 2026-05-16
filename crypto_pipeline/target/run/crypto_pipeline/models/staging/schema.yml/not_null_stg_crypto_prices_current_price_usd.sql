
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select current_price_usd
from CRYPTO_DB.STAGING.stg_crypto_prices
where current_price_usd is null



  
  
      
    ) dbt_internal_test