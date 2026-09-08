{{
    config(
        alias='stock_plan_brand1'
    )
}}

SELECT
    stock_plan.brand,
    stock_plan.planning_month,
    stock_plan.sku,
    stock_plan.quantity
FROM {{ ref('src_gsheets__stock_plan_brand1') }} AS stock_plan
