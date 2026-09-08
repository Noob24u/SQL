{{
    config(
        alias='stock_plan_brand2'
    )
}}

-- Added for this repo -- see core_brand2__weekly_stock_availability.sql for why.
SELECT
    stock_plan.brand,
    stock_plan.planning_month,
    stock_plan.sku,
    stock_plan.quantity
FROM {{ ref('src_gsheets__stock_plan_brand2') }} AS stock_plan
