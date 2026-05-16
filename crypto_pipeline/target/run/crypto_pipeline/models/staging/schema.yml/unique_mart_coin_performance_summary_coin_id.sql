
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    coin_id as unique_field,
    count(*) as n_records

from CRYPTO_DB.MARTS.mart_coin_performance_summary
where coin_id is not null
group by coin_id
having count(*) > 1



  
  
      
    ) dbt_internal_test