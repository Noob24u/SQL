{{
    config(
        alias='stock_received_brand1'
    )
}}

SELECT
    'brand1' AS brand,
    stock_received.orderno AS order_number,
    stock_received.itemno AS sku,
    stock_received.quantity,
    stock_received.vcountry AS vendor_country,
    stock_received.vendorname AS vendor_name,
    try_to_date(stock_received.deliverydate::varchar, 'ddmmyyyy') AS delivery_date,
    try_to_date(stock_received.orderdate::varchar, 'ddmmyyyy') AS order_date,
    stock_received.line_no
FROM {{ source('gsheets', 'supplier_pos_brand1') }} AS stock_received
