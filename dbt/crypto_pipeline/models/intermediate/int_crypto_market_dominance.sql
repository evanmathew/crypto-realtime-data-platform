-- int_crypto_market_dominance.sql
-- Calculates each coin's market dominance % per ingestion snapshot

{{ config(
    materialized='incremental',
    unique_key='coin_id || ingested_at',
    on_schema_change='sync_all_columns'
) }}

with staging as (
    select * from {{ ref('stg_crypto_prices') }}

    {% if is_incremental() %}

        where ingested_at >
        (
            select max(ingested_at)
            from {{ this }}
        )

    {% endif %}
    
),

total_market as (
    select
        ingested_at,
        sum(market_cap_usd) as total_market_cap_usd
    from staging
    group by ingested_at
),

dominance as (
    select
        s.coin_id,
        s.symbol,
        s.name,
        s.ingested_at,
        s.current_price_usd,
        s.market_cap_usd,
        t.total_market_cap_usd,
        round(
            (s.market_cap_usd / nullif(t.total_market_cap_usd, 0)) * 100, 4
        )                                        as market_dominance_pct,
        s.total_volume_usd,
        s.price_change_pct_24h
    from staging s
    left join total_market t
        on s.ingested_at = t.ingested_at
)

select * from dominance