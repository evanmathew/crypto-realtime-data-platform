
      
  
    

create or replace transient table CRYPTO_DB.SNAPSHOTS.snap_crypto_prices
    
    
    
    as (
    

    select *,
        md5(coalesce(cast(coin_id as varchar ), '')
         || '|' || coalesce(cast(to_timestamp_ntz(convert_timezone('UTC', current_timestamp())) as varchar ), '')
        ) as dbt_scd_id,
        to_timestamp_ntz(convert_timezone('UTC', current_timestamp())) as dbt_updated_at,
        to_timestamp_ntz(convert_timezone('UTC', current_timestamp())) as dbt_valid_from,
        
  
  coalesce(nullif(to_timestamp_ntz(convert_timezone('UTC', current_timestamp())), to_timestamp_ntz(convert_timezone('UTC', current_timestamp()))), null)
  as dbt_valid_to
from (
        



select
    coin_id,
    symbol,
    name,
    current_price_usd,
    market_cap_usd,
    market_cap_rank,
    total_volume_usd,
    price_change_pct_24h,
    all_time_high_usd,
    pct_below_ath,
    market_dominance_pct,
    last_updated_at
from CRYPTO_DB.MARTS.mart_coin_performance_summary

    ) sbq



    )
;


  
  