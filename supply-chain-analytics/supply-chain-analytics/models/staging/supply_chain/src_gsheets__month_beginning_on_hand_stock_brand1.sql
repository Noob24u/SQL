{{
    config(
        alias='month_beginning_on_hand_stock_brand1'
    )
}}

-- This inventory quantity doesn't include inventory tied up in open orders,
-- and is always up to date.
-- NOTE: the source sheet has no brand column at all -- added here (unlike
-- the other staging models in this project, which get 'brandN' from the
-- sheet's own file/source) so this can safely UNION with brand2 downstream.
SELECT
    'brand1' AS brand,
    stocks_on_hand.date AS month,
    stocks_on_hand.sku,
    stocks_on_hand.starting_inventory AS quantity
FROM {{ source('gsheets', 'month_beginning_on_hand_stock_brand1') }} AS stocks_on_hand
