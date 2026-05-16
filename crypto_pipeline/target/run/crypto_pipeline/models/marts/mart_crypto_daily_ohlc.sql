
  
    

create or replace transient table CRYPTO_DB.MARTS.mart_crypto_daily_ohlc
    
    
    
    as (-- mart_crypto_daily_ohlc.sql
-- Incremental: only reprocesses today's and yesterday's data
-- Yesterday included to handle late-arriving records



with hourly as (
    select * from CRYPTO_DB.INTERMEDIATE.int_crypto_hourly

    
),

daily as (
    select
        coin_id,
        symbol,
        name,
        date_trunc('day', hour_bucket)           as trade_date,
        max(max_price_usd)                       as high_usd,
        min(min_price_usd)                       as low_usd,
        avg(avg_price_usd)                       as avg_price_usd,
        first_value(avg_price_usd) over (
            partition by coin_id, date_trunc('day', hour_bucket)
            order by hour_bucket asc
            rows between unbounded preceding and unbounded following
        )                                        as open_usd,
        last_value(avg_price_usd) over (
            partition by coin_id, date_trunc('day', hour_bucket)
            order by hour_bucket asc
            rows between unbounded preceding and unbounded following
        )                                        as close_usd,
        sum(avg_volume_usd)                      as total_volume_usd,
        max(all_time_high_usd)                   as all_time_high_usd,
        sum(record_count)                        as total_records
    from hourly
    group by 1, 2, 3, 4, hour_bucket, avg_price_usd
)

select
    coin_id,
    symbol,
    name,
    trade_date,
    open_usd,
    high_usd,
    low_usd,
    close_usd,
    avg_price_usd,
    total_volume_usd,
    all_time_high_usd,
    round(((close_usd - open_usd) / nullif(open_usd, 0)) * 100, 4) as daily_return_pct,
    total_records
from daily
    )
;


  