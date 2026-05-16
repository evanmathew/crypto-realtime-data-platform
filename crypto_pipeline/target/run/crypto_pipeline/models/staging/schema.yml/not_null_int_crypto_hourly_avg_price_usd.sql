
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select avg_price_usd
from CRYPTO_DB.INTERMEDIATE.int_crypto_hourly
where avg_price_usd is null



  
  
      
    ) dbt_internal_test