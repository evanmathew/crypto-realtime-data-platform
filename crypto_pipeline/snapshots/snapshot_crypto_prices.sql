-- snap_crypto_prices.sql
-- Tracks how coin prices change over time (SCD Type 2)
-- Every time a coin's price changes a new record is created
-- with dbt_valid_from and dbt_valid_to timestamps

{% snapshot snap_crypto_prices %}

{{
    config(
        target_database='CRYPTO_DB',
        target_schema='SNAPSHOTS',
        unique_key='coin_id',
        strategy='check',
        check_cols=[
            'current_price_usd',
            'market_cap_usd',
            'total_volume_usd',
            'price_change_pct_24h',
            'market_dominance_pct'
            ]
    )
}}

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
from {{ ref('mart_coin_performance_summary') }}

{% endsnapshot %}