{{
    config(
        alias='weekly_stock_availability_brand2'
    )
}}

SELECT
    'brand2' AS brand,
    weekly_stock_overview.timestamp AS reporting_date,
    weekly_stock_overview.artikel AS sku,
    weekly_stock_overview.sum_bestand_ AS quantity
FROM {{ source('gsheets', 'weekly_stock_overview_brand2') }} AS weekly_stock_overview
