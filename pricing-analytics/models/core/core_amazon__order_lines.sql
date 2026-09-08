{{
    config(
        alias='order_lines'
    )
}}

-- This models brand1's sales through the Amazon marketplace channel
-- specifically (not brand1's direct-to-consumer channel, and not brand2 --
-- brand2's Amazon data wasn't part of what was uploaded for this project).
SELECT
    order_lines.id,
    order_lines.order_id,
    'brand1' AS brand,
    order_lines.country_code,
    order_lines.shipment_id,
    order_lines.shipment_item_id,
    order_lines.order_item_id,
    order_lines.customer_order_item_id,
    order_lines.created_at,
    order_lines.paid_at,
    order_lines.shipped_at,
    order_lines.reported_at,
    order_lines.sku,
    order_lines.product_name,
    order_lines.quantity_shipped,
    order_lines.currency,
    order_lines.full_price_net,
    order_lines.item_tax,
    order_lines.shipping_amount,
    order_lines.shipping_tax,
    order_lines.gift_wrap_amount,
    order_lines.gift_wrap_tax,
    order_lines.item_discount_amount,
    order_lines.shipment_discount_amount,

    order_lines.full_price_net + order_lines.item_tax AS full_price,
    order_lines.full_price_net + order_lines.item_tax
        + order_lines.item_discount_amount + order_lines.shipment_discount_amount AS price_after_discount,
    order_lines.full_price_net + order_lines.item_tax
        + order_lines.item_discount_amount + order_lines.shipment_discount_amount
        + order_lines.shipping_amount + order_lines.shipping_tax
        + order_lines.gift_wrap_amount + order_lines.gift_wrap_tax AS paid_amount,

    order_lines.full_price_net
        + order_lines.item_discount_amount + order_lines.shipment_discount_amount AS price_after_discount_net,
    order_lines.full_price_net
        + order_lines.item_discount_amount + order_lines.shipment_discount_amount
        + order_lines.shipping_amount
        + order_lines.gift_wrap_amount AS paid_amount_net,

    order_lines.item_tax + order_lines.shipping_tax + order_lines.gift_wrap_tax AS total_tax,
    order_lines.item_discount_amount + order_lines.shipment_discount_amount AS total_discount,

    (order_lines.full_price_net
        + order_lines.item_discount_amount + order_lines.shipment_discount_amount
        + order_lines.shipping_amount
        + order_lines.gift_wrap_amount) * 0.15 AS commission_cost,
    (amazon_product_costs.cost + brand1_product_costs.cost) * order_lines.quantity_shipped AS product_cost,
    CASE WHEN fba_fees.currency = 'PLN' THEN fba_fees.expected_domestic_fulfilment_fee_per_unit * 0.23
         ELSE fba_fees.expected_domestic_fulfilment_fee_per_unit END * order_lines.quantity_shipped AS fba_fee,

    order_lines.shipment_service_type,
    order_lines.shipping_address_country_code,
    order_lines.carrier,
    order_lines.tracking_number,
    order_lines.estimated_arrival_date,
    order_lines.fulfillment_center_id,
    order_lines.fulfillment_channel
FROM {{ ref('src_gcs_amazon__order_lines') }} AS order_lines
LEFT JOIN {{ ref('core_amazon__variant_product_costs') }} AS amazon_product_costs
    ON order_lines.sku = amazon_product_costs.sku
    AND DATE_TRUNC('month', order_lines.created_at) = amazon_product_costs.month_start_date
LEFT JOIN {{ ref('core_brand1__variant_product_costs') }} AS brand1_product_costs
    ON order_lines.sku = brand1_product_costs.sku
    AND DATE_TRUNC('month', order_lines.created_at) = brand1_product_costs.month_start_date
LEFT JOIN {{ ref('amazon_fba_fees_seed') }} AS fba_fees
    ON order_lines.country_code = fba_fees.country
    AND order_lines.sku = fba_fees.sku
