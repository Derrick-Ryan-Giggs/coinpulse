with source as (
    select * from {{ source('raw_streaming', 'stream_prices') }}
),

renamed as (
    select
        upper(symbol)                      as symbol,
        price_usd,
        avg_price,
        min_price,
        max_price,
        price_stddev,
        open_price,
        close_price,
        record_count,
        cast(window_start as timestamp)    as window_start,
        cast(window_end as timestamp)      as window_end,
        cast(event_timestamp as timestamp) as event_timestamp,
        date(event_timestamp)              as event_date

    from source
    where symbol is not null
      and price_usd > 0
)

select * from renamed