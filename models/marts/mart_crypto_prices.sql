{{
  config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={
      "field": "event_timestamp",
      "data_type": "timestamp",
      "granularity": "hour"
    },
    cluster_by=["symbol"]
  )
}}

with streaming as (
    select * from {{ ref('stg_stream_prices') }}
    {% if is_incremental() %}
        where event_timestamp >= timestamp_sub(current_timestamp(), interval 2 hour)
    {% endif %}
),

markets as (
    select * from {{ ref('stg_coingecko_markets') }}
),

-- Get latest market metadata per symbol
-- snapshot_date is now a proper DATE (cast in staging), so ordering is reliable
latest_market as (
    select *
    from markets
    qualify row_number() over (
        partition by symbol
        order by snapshot_date desc
    ) = 1
),

joined as (
    select
        s.symbol,
        s.price_usd,
        s.avg_price,
        s.min_price,
        s.max_price,
        s.price_stddev,
        s.open_price,
        s.close_price,
        s.record_count,
        s.window_start,
        s.window_end,
        s.event_timestamp,
        s.event_date,
        -- Batch enrichment (nulls expected until streaming pipeline runs)
        m.name,
        m.market_cap,
        m.volume_24h,
        m.price_change       as daily_price_change_pct,
        m.rank               as market_cap_rank,
        m.fdv,
        m.market_cap_category

    from streaming s
    left join latest_market m
        on s.symbol = m.symbol
)

select * from joined