{{
    config(
        alias='weekly_stock_availability_brand1'
    )
}}

SELECT

    brand,
    reporting_date,
    sku,
    quantity
FROM {{ ref('src_gsheets__weekly_stock_availability_brand1') }}
