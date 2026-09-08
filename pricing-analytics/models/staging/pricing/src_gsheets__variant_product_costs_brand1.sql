{{
    config(
        alias='variant_product_costs_brand1'
    )
}}

-- Same source and shape as the Supply Chain Analytics project's copy of
-- this model -- brand1's direct/base landed cost, needed here because
-- Amazon-channel order margin combines this with the Amazon-specific cost
-- (see core_amazon__order_lines).
WITH json_product_cost AS (

    SELECT
        product_cost.sku,
        object_construct_keep_null(*) AS json_product_cost
    FROM {{ source('gsheets', 'product_cost_brand1') }} AS product_cost

)

SELECT
    MD5(CONCAT(json_product_cost.sku, flatten.key)) AS id,
    json_product_cost.sku,
    REPLACE(LTRIM(flatten.key, '_'), '_', '-')::DATE AS month_start_date,
    flatten.value::FLOAT AS cost
FROM json_product_cost,
LATERAL FLATTEN(input => json_product_cost.json_product_cost) flatten
WHERE flatten.key NOT IN ('PRODUCT_NAME', 'SKU', '_FIVETRAN_SYNCED', '_ROW')
    AND flatten.value::FLOAT IS NOT NULL
