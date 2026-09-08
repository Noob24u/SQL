{{
    config(
        alias='month_beginning_on_hand_stock'
    )
}}

-- This inventory quantity doesn't include inventory tied up in open orders.
SELECT
    brand,
    month,
    sku,
    quantity
FROM {{ ref('src_gsheets__month_beginning_on_hand_stock_brand1') }}

UNION ALL

SELECT
    brand,
    month,
    sku,
    quantity
FROM {{ ref('src_gsheets__month_beginning_on_hand_stock_brand2') }}
