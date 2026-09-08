{{
    config(
        alias='variant_product_costs_brand2'
    )
}}

-- Added for this repo -- see core_brand1__variant_product_costs.sql for why.
SELECT
    product_costs.id,
    product_costs.month_start_date,
    product_costs.sku,
    product_costs.cost
FROM {{ ref('src_gsheets__variant_product_costs_brand2') }} AS product_costs
