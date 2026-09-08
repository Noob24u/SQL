{{
    config(
        alias='amazon_sku_margin'
    )
}}

-- Grain: one row per brand + SKU + country + calendar month.
-- Contribution margin = net revenue collected, minus landed product cost,
-- Amazon referral commission, and the FBA fulfilment fee. It does NOT
-- subtract freight/palletizing logistics costs (core_amazon__logistic_costs)
-- -- that data is at the shipment level, not SKU level, and allocating it
-- down to a per-SKU number needs an allocation method (per unit? per
-- shipment? by weight?) that isn't specified anywhere in the source data,
-- so it's left out rather than guessed at. See the project README's
-- "Known limitations" for what that means for reading this number.
WITH order_lines AS (

    SELECT *
    FROM {{ ref('core_amazon__order_lines') }}

)

SELECT
    order_lines.brand,
    order_lines.sku,
    order_lines.country_code,
    DATE_TRUNC('month', order_lines.created_at)::DATE AS month,

    SUM(order_lines.quantity_shipped) AS units_sold,
    SUM(order_lines.paid_amount_net) AS net_revenue,
    SUM(order_lines.product_cost) AS product_cost,
    SUM(order_lines.commission_cost) AS commission_cost,
    SUM(order_lines.fba_fee) AS fba_fee,

    SUM(order_lines.paid_amount_net)
        - SUM(order_lines.product_cost)
        - SUM(order_lines.commission_cost)
        - SUM(order_lines.fba_fee) AS contribution_margin,

    DIV0(
        SUM(order_lines.paid_amount_net)
            - SUM(order_lines.product_cost)
            - SUM(order_lines.commission_cost)
            - SUM(order_lines.fba_fee),
        SUM(order_lines.paid_amount_net)
    ) AS contribution_margin_pct,

    DIV0(SUM(order_lines.paid_amount_net), SUM(order_lines.quantity_shipped)) AS avg_net_selling_price
FROM order_lines
GROUP BY 1, 2, 3, 4
