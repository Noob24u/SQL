{{
    config(
        alias='variant_product_costs_amazon'
    )
}}

-- The source sheet is wide: one column per month (e.g. `_2024_01_01`),
-- added by hand each month rather than the sheet being reshaped. We take
-- every column ('*') instead of naming them explicitly, because naming them
-- would mean updating this model every single month as columns are added.
-- NOTE: unlike the brand1/brand2 versions of this model (Supply Chain
-- Analytics project), this one's exclusion list doesn't include
-- 'PRODUCT_NAME' -- if this sheet ever gains that column, the ::FLOAT cast
-- below will error. See the project README.
WITH json_product_cost AS (

    SELECT
        product_cost.sku,
        object_construct_keep_null(*) AS json_product_cost
    FROM {{ source('gsheets', 'product_cost_amazon') }} AS product_cost

)

SELECT
    MD5(CONCAT(json_product_cost.sku, flatten.key)) AS id,
    json_product_cost.sku,
    REPLACE(LTRIM(flatten.key, '_'), '_', '-')::DATE AS month_start_date,
    flatten.value::FLOAT AS cost
FROM json_product_cost,
LATERAL FLATTEN(input => json_product_cost.json_product_cost) flatten
WHERE flatten.key NOT IN ('SKU', '_FIVETRAN_SYNCED', '_ROW')
    AND flatten.value::FLOAT IS NOT NULL
