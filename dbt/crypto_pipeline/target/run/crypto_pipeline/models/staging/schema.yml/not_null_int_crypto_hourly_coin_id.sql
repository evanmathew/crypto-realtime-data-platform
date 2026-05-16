
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select coin_id
from CRYPTO_DB.INTERMEDIATE.int_crypto_hourly
where coin_id is null



  
  
      
    ) dbt_internal_test