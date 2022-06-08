WITH primary_cte AS (
    SELECT 
        weekly_stock_availability.brand,
        DATE_TRUNC(week,weekly_stock_availability.reporting_date::DATE) AS reporting_week,
        weekly_stock_availability.reporting_date::DATE AS reporting_date,
        weekly_stock_availability.sku,
        Extract (year, reporting_date::DATE) as year,
        weekly_stock_availability.quantity AS available_inventory,
        prod_cost.month_start_date::DATE as cost_date,
        prod_cost.cost AS prod_cost
    FROM dbt.prod_schema_brand1.prod_cost
    LEFT JOIN dbt.prod_core_am.weekly_stock_availability
        ON weekly_stock_availability.sku = variant_product_costs.sku
    WHERE DATE_TRUNC(month,weekly_stock_availability.reporting_date::DATE) = DATE_TRUNC(month, variant_product_costs.month_start_date::DATE )
    -- ORDER BY weekly_stock_availability.reporting_date::date DESC
    
    union all
    
    SELECT 
        weekly_stock_availability.brand,
        DATE_TRUNC(week,weekly_stock_availability.reporting_date::DATE) AS reporting_week,
        weekly_stock_availability.reporting_date::DATE AS reporting_date,
        weekly_stock_availability.sku,
        Extract (year, reporting_date::DATE) as year,
        weekly_stock_availability.quantity AS available_inventory,
        variant_product_costs.month_start_date::DATE as cost_date,
        variant_product_costs.cost AS prod_cost
    FROM dbt.prod_schema_brand2.prod_cost
    LEFT JOIN dbt.prod_core_faynt.weekly_stock_availability
        ON weekly_stock_availability.sku = variant_product_costs.sku
    WHERE DATE_TRUNC(month,weekly_stock_availability.reporting_date::DATE) = DATE_TRUNC(month, variant_product_costs.month_start_date::DATE )
)

, sku_value_pw AS (
     SELECT
        brand,
        sku,
        available_inventory,
        reporting_week,
        reporting_date,
        prod_cost,
        year,
        available_inventory * prod_cost AS stock_value
    FROM primary_tbl
    WHERE reporting_week = DATE_TRUNC('week', current_date::DATE)
    GROUP BY 1, 2, 3, 4, 5, 6, 7
    )
, sku_value_py AS (
    SELECT
        brand,
        sku,
        available_inventory,
        reporting_week,
        reporting_date,
        prod_cost,
        year,
        available_inventory * prod_cost AS stock_value
    FROM primary_tbl
    WHERE reporting_week = DATEADD('week', -52, DATE_TRUNC('week', current_date::DATE))
    GROUP BY 1, 2, 3, 4, 5, 6, 7
)
, pw AS (
    SELECT
        brand,
        sku_value_pw.reporting_week,
        SUM(sku_value_pw.stock_value) AS inventory_pw_value
    FROM sku_value_pw
    GROUP BY 1, 2

)
, py AS (
    SELECT
        brand,
        sku_value_py.reporting_week,
        SUM(sku_value_py.stock_value) AS inventory_py_value
    FROM sku_value_py
    GROUP BY 1,2
)
, final_agg as (
SELECT 
    pw.brand,
    py.inventory_py_value,
    pw.inventory_pw_value,
    DIV0(pw.inventory_pw_value, py.inventory_py_value) AS pw_py_stock,
    DIV0((pw.inventory_pw_value - py.inventory_py_value), py.inventory_py_value) as pw_py_percent
FROM pw
    LEFT join py
        On pw.brand = py.brand
GROUP by 1, 2, 3, 4, 5
)
, final_cal AS (
SELECT 
    brand,
    SUM(inventory_py_value) AS inventory_py_value,
    SUM(inventory_pw_value) AS inventory_pw_value,
    DIV0((SUM(inventory_pw_value) - SUM(inventory_py_value)), SUM(inventory_py_value)) as pw_py_percent
    FROM final_agg
GROUP BY 1
union all
SELECT 
    'Total' AS brand,
    SUM(inventory_py_value) AS inventory_py_value,
    SUM(inventory_pw_value) AS inventory_pw_value,
    DIV0((SUM(inventory_pw_value) - SUM(inventory_py_value)), SUM(inventory_py_value)) as pw_py_percent
FROM final_agg
GROUP by 1
)
SELECT
    brand,
    -- ROUND(inventory_py_value, 0) as "p/Y this week",
    -- ROUND( inventory_pw_value, 0)  AS "On Current Week",
    pw_py_percent AS "Stock Growth"
FROM final_cal
