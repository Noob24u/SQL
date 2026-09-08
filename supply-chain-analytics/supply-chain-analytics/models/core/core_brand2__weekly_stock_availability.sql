{{
    config(
        alias='weekly_stock_availability_brand2'
    )
}}

-- Added for this repo: the original codebase only had a core model for
-- brand1 (core_am__weekly_stock_availability selected from brand1's staging
-- model alone, with no brand2 counterpart). This mirrors that model's exact
-- shape for brand2, since the staging model already exists and inventory_growth
-- needs both brands.
SELECT

    brand,
    reporting_date,
    sku,
    quantity
FROM {{ ref('src_gsheets__weekly_stock_availability_brand2') }}
