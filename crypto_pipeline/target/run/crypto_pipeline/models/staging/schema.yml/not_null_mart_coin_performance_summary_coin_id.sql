
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select coin_id
from CRYPTO_DB.MARTS.mart_coin_performance_summary
where coin_id is null



  
  
      
    ) dbt_internal_test