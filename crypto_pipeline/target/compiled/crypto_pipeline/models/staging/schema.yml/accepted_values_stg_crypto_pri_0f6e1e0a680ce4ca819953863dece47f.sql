
    
    

with all_values as (

    select
        symbol as value_field,
        count(*) as n_records

    from CRYPTO_DB.STAGING.stg_crypto_prices
    group by symbol

)

select *
from all_values
where value_field not in (
    'btc','eth','sol','bnb','xrp'
)


