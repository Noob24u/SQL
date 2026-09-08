{{
    config(
        alias='weekly_stock_availability_brand1'
    )
}}

SELECT
    'brand1' AS brand,
    weekly_stock_overview.timestamp AS reporting_date,
    weekly_stock_overview.artikel AS sku,
    weekly_stock_overview.sum_bestand_ AS quantity
FROM {{ source('gsheets', 'weekly_stock_overview_brand1') }} AS weekly_stock_overview
