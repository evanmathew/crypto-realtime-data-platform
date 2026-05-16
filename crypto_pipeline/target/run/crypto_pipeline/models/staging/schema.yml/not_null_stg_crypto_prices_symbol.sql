
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select symbol
from CRYPTO_DB.STAGING.stg_crypto_prices
where symbol is null



  
  
      
    ) dbt_internal_test