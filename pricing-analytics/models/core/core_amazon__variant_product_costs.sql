{{
    config(
        alias='variant_product_costs_amazon'
    )
}}

SELECT
    product_costs.id,
    product_costs.month_start_date,
    product_costs.sku,
    product_costs.cost
FROM {{ ref('src_gsheets__variant_product_costs_amazon') }} AS product_costs
