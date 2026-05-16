
    
    

select
    coin_id as unique_field,
    count(*) as n_records

from CRYPTO_DB.MARTS.mart_coin_performance_summary
where coin_id is not null
group by coin_id
having count(*) > 1


