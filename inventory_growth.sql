WITH primary_cte AS (
    SELECT 
        weekly_stock.brand,
        DATE_TRUNC(week,weekly_stock.reporting_date::DATE) AS reporting_week,
        weekly_stock.sku,
        Extract (year, reporting_date::DATE) as year,
        weekly_stock.quantity AS available_stock,
        prod_cost.month_start_date::DATE as cost_date,
        prod_cost.cost AS prod_cost
    FROM dbt.prod_schema_brand1.prod_cost
    LEFT JOIN dbt.prod_schema_brand1.weekly_stock
        ON weekly_stock_availability.sku = variant_product_costs.sku
    WHERE DATE_TRUNC(month,weekly_stock_availability.reporting_date::DATE) = DATE_TRUNC(month, variant_product_costs.month_start_date::DATE )
    -- ORDER BY weekly_stock_availability.reporting_date::date DESC
    
    union all
    
    SELECT 
        weekly_stock.brand,
        DATE_TRUNC(week,weekly_stock.reporting_date::DATE) AS reporting_week,
        weekly_stock.sku,
        Extract (year, reporting_date::DATE) as year,
        weekly_stock_availability.quantity AS available_stock,
        prod_cost.month_start_date::DATE as cost_date,
        prod_cost.cost AS prod_cost
    FROM dbt.prod_schema_brand2.prod_cost
    LEFT JOIN dbt.prod_schema_brand2.weekly_stock
        ON weekly_stock_availability.sku = variant_product_costs.sku
    WHERE DATE_TRUNC(month,weekly_stock_availability.reporting_date::DATE) = DATE_TRUNC(month, variant_product_costs.month_start_date::DATE )
)

, stock_value_pw AS (
     SELECT
        primary_cte.brand,
        primary_cte.sku,
        primary_cte.available_stock,
        primary_cte.reporting_week,
        primary_cte.prod_cost,
        primary_cte.year,
        primary_cte.available_stock * primary_cte.prod_cost AS stock_value
    FROM primary_cte
    WHERE reporting_week = DATE_TRUNC('week', current_date::DATE)
    GROUP BY 1, 2, 3, 4, 5, 6, 7
)

, stock_value_py AS (
    SELECT
        primary_cte.brand,
        primary_cte.sku,
        primary_cte.available_stock,
        primary_cte.reporting_week,
        primary_cte.prod_cost,
        primary_cte.year,
        primary_cte.available_stock * primary_cte.prod_cost AS stock_value
    FROM primary_cte
    WHERE reporting_week = DATEADD('week', -52, DATE_TRUNC('week', current_date::DATE))
    GROUP BY 1, 2, 3, 4, 5, 6, 7
)

, pw_stock AS (
    SELECT
        sku_value_pw.brand,
        sku_value_pw.reporting_week,
        SUM(sku_value_pw.stock_value) AS inventory_pw_value
    FROM stock_value_pw
    GROUP BY 1, 2

)

, py_stock AS (
    SELECT
        sku_value_pw.brand,
        sku_value_py.reporting_week,
        SUM(sku_value_py.stock_value) AS inventory_py_value
    FROM stock_value_py
    GROUP BY 1,2
)

, pw_py_percent as (
SELECT 
    pw.brand,
    py.inventory_py_value,
    pw.inventory_pw_value,
    DIV0(pw.inventory_pw_value, py.inventory_py_value) AS pw_py_stock,
    DIV0((pw.inventory_pw_value - py.inventory_py_value), py.inventory_py_value) as pw_py_percent
FROM pw_stock
    LEFT join py
        On pw.brand = py.brand
GROUP by 1, 2, 3, 4, 5
)
, final_calculation AS (
SELECT 
    brand,
    SUM(inventory_py_value) AS inventory_py_value,
    SUM(inventory_pw_value) AS inventory_pw_value,
    DIV0((SUM(inventory_pw_value) - SUM(inventory_py_value)), SUM(inventory_py_value)) as pw_py_percent
    FROM pw_py_percent
GROUP BY 1
union all
SELECT 
    'Total' AS brand,
    SUM(inventory_py_value) AS inventory_py_value,
    SUM(inventory_pw_value) AS inventory_pw_value,
    DIV0((SUM(inventory_pw_value) - SUM(inventory_py_value)), SUM(inventory_py_value)) as pw_py_percent
FROM pw_py_percent
GROUP by 1
)
SELECT
    brand,
    -- ROUND(inventory_py_value, 0) as "p/Y this week",
    -- ROUND( inventory_pw_value, 0)  AS "On Current Week",
    pw_py_percent AS "Stock Growth"
FROM final_calculation
