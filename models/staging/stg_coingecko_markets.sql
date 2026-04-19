with source as (
    select * from {{ source('raw_batch', 'coingecko_ohlcv') }}
),

renamed as (
    select
        upper(symbol)                                  as symbol,
        name,
        price_usd,
        market_cap,
        volume_24h,
        price_change,
        rank,
        fdv,
        -- Cast STRING → DATE for reliable ordering in marts
        parse_date('%Y-%m-%d', snapshot_date)          as snapshot_date,
        ingested_at,
        -- Derive market cap category
        case
            when market_cap >= 10000000000  then 'large_cap'
            when market_cap >= 1000000000   then 'mid_cap'
            when market_cap >= 100000000    then 'small_cap'
            else 'micro_cap'
        end                                            as market_cap_category

    from source
    where symbol is not null
      and price_usd > 0
)

select * from renamed