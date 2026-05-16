
  
    

create or replace transient table CRYPTO_DB.INTERMEDIATE.int_crypto_hourly
    
    
    
    as (-- int_crypto_hourly.sql
-- Aggregates staging data into hourly snapshots per coin



with staging as (
    
    select *
    from CRYPTO_DB.STAGING.stg_crypto_prices

    
),

hourly as (
    select
        coin_id,
        symbol,
        name,
        date_trunc('hour', ingested_at)         as hour_bucket,
        avg(current_price_usd)                   as avg_price_usd,
        max(current_price_usd)                   as max_price_usd,
        min(current_price_usd)                   as min_price_usd,
        avg(market_cap_usd)                      as avg_market_cap_usd,
        avg(total_volume_usd)                    as avg_volume_usd,
        avg(price_change_pct_24h)                as avg_price_change_pct_24h,
        max(all_time_high_usd)                   as all_time_high_usd,
        min(all_time_low_usd)                    as all_time_low_usd,
        count(*)                                 as record_count
    from staging
    group by 1, 2, 3, 4
)

select * from hourly
    )
;


  