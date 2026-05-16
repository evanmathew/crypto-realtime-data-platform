
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select market_dominance_pct
from CRYPTO_DB.MARTS.mart_coin_performance_summary
where market_dominance_pct is null



  
  
      
    ) dbt_internal_test