{{
    config(
        alias='variant_product_costs_brand1'
    )
}}

-- Added for this repo, mirroring the shape of core_amazon__variant_product_costs
-- (a thin passthrough of the staging model) so inventory_growth has a proper
-- ref()-able cost model for brand1 instead of an ad hoc schema reference.
SELECT
    product_costs.id,
    product_costs.month_start_date,
    product_costs.sku,
    product_costs.cost
FROM {{ ref('src_gsheets__variant_product_costs_brand1') }} AS product_costs
