{{
    config(
        alias='inventory_growth'
    )
}}

WITH stock_and_cost AS (

    SELECT
        weekly_stock.brand,
        DATE_TRUNC('week', weekly_stock.reporting_date::DATE) AS reporting_week,
        weekly_stock.sku,
        DATE_PART('year', weekly_stock.reporting_date::DATE) AS stock_year,
        weekly_stock.quantity AS available_stock,
        product_cost.month_start_date::DATE AS cost_date,
        product_cost.cost AS product_cost
    FROM {{ ref('core_brand1__weekly_stock_availability') }} AS weekly_stock
    LEFT JOIN {{ ref('core_brand1__variant_product_costs') }} AS product_cost
        ON weekly_stock.sku = product_cost.sku
        AND DATE_TRUNC('month', weekly_stock.reporting_date::DATE) = DATE_TRUNC('month', product_cost.month_start_date::DATE)

    UNION ALL

    SELECT
        weekly_stock.brand,
        DATE_TRUNC('week', weekly_stock.reporting_date::DATE) AS reporting_week,
        weekly_stock.sku,
        DATE_PART('year', weekly_stock.reporting_date::DATE) AS stock_year,
        weekly_stock.quantity AS available_stock,
        product_cost.month_start_date::DATE AS cost_date,
        product_cost.cost AS product_cost
    FROM {{ ref('core_brand2__weekly_stock_availability') }} AS weekly_stock
    LEFT JOIN {{ ref('core_brand2__variant_product_costs') }} AS product_cost
        ON weekly_stock.sku = product_cost.sku
        AND DATE_TRUNC('month', weekly_stock.reporting_date::DATE) = DATE_TRUNC('month', product_cost.month_start_date::DATE)

),

stock_value_pw AS (

    SELECT
        stock_and_cost.brand,
        stock_and_cost.sku,
        stock_and_cost.available_stock,
        stock_and_cost.reporting_week,
        stock_and_cost.product_cost,
        stock_and_cost.stock_year,
        stock_and_cost.available_stock * stock_and_cost.product_cost AS stock_value
    FROM stock_and_cost
    WHERE stock_and_cost.reporting_week = DATE_TRUNC('week', CURRENT_DATE::DATE)
    GROUP BY 1, 2, 3, 4, 5, 6, 7

),

stock_value_py AS (

    SELECT
        stock_and_cost.brand,
        stock_and_cost.sku,
        stock_and_cost.available_stock,
        stock_and_cost.reporting_week,
        stock_and_cost.product_cost,
        stock_and_cost.stock_year,
        stock_and_cost.available_stock * stock_and_cost.product_cost AS stock_value
    FROM stock_and_cost
    WHERE stock_and_cost.reporting_week = DATEADD('week', -52, DATE_TRUNC('week', CURRENT_DATE::DATE))
    GROUP BY 1, 2, 3, 4, 5, 6, 7

),

pw_stock AS (

    SELECT
        stock_value_pw.brand,
        stock_value_pw.reporting_week,
        SUM(stock_value_pw.stock_value) AS inventory_pw_value
    FROM stock_value_pw
    GROUP BY 1, 2

),

py_stock AS (

    SELECT
        stock_value_py.brand,
        stock_value_py.reporting_week,
        SUM(stock_value_py.stock_value) AS inventory_py_value
    FROM stock_value_py
    GROUP BY 1, 2

),

pw_py_percent AS (

    SELECT
        pw.brand,
        py.inventory_py_value,
        pw.inventory_pw_value,
        DIV0(pw.inventory_pw_value, py.inventory_py_value) AS pw_py_stock,
        DIV0((pw.inventory_pw_value - py.inventory_py_value), py.inventory_py_value) AS pw_py_percent
    FROM pw_stock AS pw
    LEFT JOIN py_stock AS py
        ON pw.brand = py.brand
    GROUP BY 1, 2, 3, 4, 5

),

final_calculation AS (

    SELECT
        brand,
        SUM(inventory_py_value) AS inventory_py_value,
        SUM(inventory_pw_value) AS inventory_pw_value,
        DIV0((SUM(inventory_pw_value) - SUM(inventory_py_value)), SUM(inventory_py_value)) AS pw_py_percent
    FROM pw_py_percent
    GROUP BY 1

    UNION ALL

    SELECT
        'Total' AS brand,
        SUM(inventory_py_value) AS inventory_py_value,
        SUM(inventory_pw_value) AS inventory_pw_value,
        DIV0((SUM(inventory_pw_value) - SUM(inventory_py_value)), SUM(inventory_py_value)) AS pw_py_percent
    FROM pw_py_percent
    GROUP BY 1

)

SELECT
    brand,
    -- ROUND(inventory_py_value, 0) AS "P/Y This Week",
    -- ROUND(inventory_pw_value, 0) AS "On Current Week",
    pw_py_percent AS "Stock Growth"
FROM final_calculation
