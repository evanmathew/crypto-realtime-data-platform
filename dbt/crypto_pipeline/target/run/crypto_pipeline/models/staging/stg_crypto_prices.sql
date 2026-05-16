
  create or replace   view CRYPTO_DB.STAGING.stg_crypto_prices
  
  
  
  
  as (
    -- stg_crypto_prices.sql
-- Cleans and flattens raw CoinGecko JSON into typed columns

with source as (
    select * from CRYPTO_DB.RAW.crypto_prices_raw
),

flattened as (
    select
        coin_id,
        raw_data:id::varchar                          as coin_id_raw,
        raw_data:symbol::varchar                      as symbol,
        raw_data:name::varchar                        as name,
        raw_data:current_price::float                 as current_price_usd,
        raw_data:market_cap::float                    as market_cap_usd,
        raw_data:market_cap_rank::int                 as market_cap_rank,
        raw_data:total_volume::float                  as total_volume_usd,
        raw_data:high_24h::float                      as high_24h_usd,
        raw_data:low_24h::float                       as low_24h_usd,
        raw_data:price_change_24h::float              as price_change_24h_usd,
        raw_data:price_change_percentage_24h::float   as price_change_pct_24h,
        raw_data:circulating_supply::float            as circulating_supply,
        raw_data:total_supply::float                  as total_supply,
        raw_data:ath::float                           as all_time_high_usd,
        raw_data:atl::float                           as all_time_low_usd,
        raw_data:last_updated::timestamp_ntz          as last_updated_at,
        ingested_at,
        source
    from source
)

select * from flattened
  );

