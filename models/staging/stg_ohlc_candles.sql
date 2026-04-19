with source as (
    select * from {{ source('raw_batch', 'coingecko_ohlc_candles') }}
),

renamed as (
    select
        upper(symbol)                                        as symbol,
        -- DAG writes ISO strings e.g. "2026-04-15T10:30:00"
        -- safe_cast avoids hard failure on malformed rows
        safe_cast(candle_time as timestamp)                  as candle_time,
        date(safe_cast(candle_time as timestamp))            as candle_date,
        open,
        high,
        low,
        close,
        -- Derived metrics
        round(high - low, 6)                                 as candle_range,
        round(close - open, 6)                               as candle_change,
        round(
            safe_divide(close - open, open) * 100,
            4
        )                                                    as candle_change_pct,
        case
            when close >= open then 'bullish'
            else 'bearish'
        end                                                  as candle_direction,
        parse_date('%Y-%m-%d', snapshot_date)                as snapshot_date,
        ingested_at

    from source
    where symbol is not null
      and close > 0
)

select * from renamed