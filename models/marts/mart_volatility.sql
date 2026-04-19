{{
  config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={
      "field": "window_hour",
      "data_type": "timestamp",
      "granularity": "hour"
    },
    cluster_by=["symbol"]
  )
}}

with streaming as (
    select * from {{ ref('stg_stream_prices') }}
    {% if is_incremental() %}
        where event_timestamp >= timestamp_sub(current_timestamp(), interval 6 hour)
    {% endif %}
),

ohlc as (
    select * from {{ ref('stg_ohlc_candles') }}
),

-- Hourly volatility from streaming windows
streaming_volatility as (
    select
        symbol,
        timestamp_trunc(event_timestamp, hour) as window_hour,
        date(event_timestamp)                  as window_date,
        avg(price_stddev)                      as avg_price_stddev,
        max(max_price) - min(min_price)        as hourly_range,
        round(
            safe_divide(
                max(max_price) - min(min_price),
                avg(avg_price)
            ) * 100,
            4
        )                                      as hourly_range_pct,
        count(*)                               as window_count,
        avg(avg_price)                         as hourly_avg_price,
        min(min_price)                         as hourly_low,
        max(max_price)                         as hourly_high

    from streaming
    group by 1, 2, 3
),

-- Daily candle metrics from batch
-- Aggregated at DAY level (matches OHLC granularity) then joined on date
-- Avoids the hourly truncation producing mostly-null joins
candle_metrics as (
    select
        symbol,
        candle_date,
        avg(candle_range)                          as avg_candle_range,
        avg(candle_change_pct)                     as avg_candle_change_pct,
        countif(candle_direction = 'bullish')      as bullish_candles,
        countif(candle_direction = 'bearish')      as bearish_candles,
        count(*)                                   as total_candles

    from ohlc
    group by 1, 2
),

joined as (
    select
        sv.symbol,
        sv.window_hour,
        sv.avg_price_stddev,
        sv.hourly_range,
        sv.hourly_range_pct,
        sv.window_count,
        sv.hourly_avg_price,
        sv.hourly_low,
        sv.hourly_high,
        cm.avg_candle_range,
        cm.avg_candle_change_pct,
        cm.bullish_candles,
        cm.bearish_candles,
        cm.total_candles,
        -- Composite volatility score
        round(
            coalesce(sv.hourly_range_pct, 0) +
            coalesce(abs(cm.avg_candle_change_pct), 0),
            4
        )                                          as composite_volatility_score

    from streaming_volatility sv
    -- Join on symbol + date (not hour) to match daily OHLC granularity
    left join candle_metrics cm
        on sv.symbol   = cm.symbol
        and sv.window_date = cm.candle_date
)

select * from joined