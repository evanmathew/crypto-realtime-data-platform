
  
    

create or replace transient table CRYPTO_DB.MARTS.mart_coin_performance_summary
    
    
    
    as (-- mart_coin_performance_summary.sql
-- Incremental: upserts latest coin snapshot



with staging as (
    select * from CRYPTO_DB.STAGING.stg_crypto_prices

    
),

dominance as (
    select * from CRYPTO_DB.INTERMEDIATE.int_crypto_market_dominance

    
),

latest as (
    select *
    from staging
    qualify row_number() over (
        partition by coin_id
        order by ingested_at desc
    ) = 1
),

latest_dominance as (
    select *
    from dominance
    qualify row_number() over (
        partition by coin_id
        order by ingested_at desc
    ) = 1
),

final as (
    select
        l.coin_id,
        l.symbol,
        l.name,
        l.current_price_usd,
        l.market_cap_usd,
        l.market_cap_rank,
        l.total_volume_usd,
        l.high_24h_usd,
        l.low_24h_usd,
        l.price_change_24h_usd,
        l.price_change_pct_24h,
        l.circulating_supply,
        l.total_supply,
        l.all_time_high_usd,
        l.all_time_low_usd,
        round(
            ((l.current_price_usd - l.all_time_high_usd)
            / nullif(l.all_time_high_usd, 0)) * 100, 2
        )                                        as pct_below_ath,
        d.market_dominance_pct,
        l.ingested_at                            as last_updated_at
    from latest l
    left join latest_dominance d
        on l.coin_id = d.coin_id
)

select * from final
    )
;


  