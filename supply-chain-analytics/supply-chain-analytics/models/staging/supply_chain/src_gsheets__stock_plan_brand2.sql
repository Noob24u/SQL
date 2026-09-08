{{
    config(
        alias='stock_plan_brand2'
    )
}}

SELECT
    'brand2' AS brand,
    stock_plan.planning_date AS planning_month,
    stock_plan.sku,
    stock_plan._quantity AS quantity
FROM {{ source('gsheets', 'stock_plan_brand2') }} AS stock_plan
