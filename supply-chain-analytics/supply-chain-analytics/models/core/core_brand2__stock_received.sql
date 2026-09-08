{{
    config(
        alias='stock_received_brand2'
    )
}}

-- Added for this repo -- see core_brand2__weekly_stock_availability.sql for why.
SELECT
    stock_received.brand,
    DATE_TRUNC('month', stock_received.order_date)::DATE AS planning_month,
    stock_received.order_number,
    stock_received.sku,
    stock_received.quantity,
    stock_received.vendor_name,
    stock_received.vendor_country,
    stock_received.delivery_date,
    stock_received.order_date
FROM {{ ref('src_gsheets__stock_received_brand2') }} AS stock_received
