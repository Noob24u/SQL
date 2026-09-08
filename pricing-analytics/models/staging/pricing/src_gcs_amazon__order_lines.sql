{{
    config(
        alias='order_lines'
    )
}}

-- NOTE: the raw source also carries buyer_email, buyer_name,
-- buyer_phone_number, recipient_name, and full ship-to/bill-to address
-- columns. None of those are selected here -- dropped at this layer since
-- a pricing/margin model has no use for customer PII. See the project
-- README's "What was changed for this repo" section.
SELECT
    MD5(CONCAT(order_lines.amazon_order_id, order_lines.sales_channel, order_lines.amazon_order_item_id, order_lines.shipment_item_id)) AS id,
    order_lines.amazon_order_id AS order_id,
    RIGHT(order_lines.sales_channel, 2) AS country_code,
    order_lines.shipment_id,
    order_lines.shipment_item_id,
    order_lines.amazon_order_item_id AS order_item_id,
    order_lines.merchant_order_item_id AS customer_order_item_id,
    order_lines.purchase_date::DATE AS created_at,
    order_lines.payments_date::DATE AS paid_at,
    order_lines.shipment_date::DATE AS shipped_at,
    order_lines.reporting_date::DATE AS reported_at,
    REPLACE(order_lines.sku, '-FBA') AS sku, -- some skus have FBA behind them, this is removed here until there is a use case for needing them
    order_lines.product_name,
    order_lines.quantity_shipped::FLOAT AS quantity_shipped,
    order_lines.currency,
    order_lines.item_price::FLOAT AS full_price_net,
    order_lines.item_tax::FLOAT AS item_tax,
    order_lines.shipping_price::FLOAT AS shipping_amount,
    order_lines.shipping_tax::FLOAT AS shipping_tax,
    order_lines.gift_wrap_price::FLOAT AS gift_wrap_amount,
    order_lines.gift_wrap_tax::FLOAT AS gift_wrap_tax,
    order_lines.ship_service_level AS shipment_service_type,
    order_lines.ship_country AS shipping_address_country_code,
    order_lines.item_promotion_discount AS item_discount_amount,
    order_lines.ship_promotion_discount AS shipment_discount_amount,
    order_lines.carrier,
    order_lines.tracking_number,
    order_lines.estimated_arrival_date::DATE AS estimated_arrival_date,
    order_lines.fulfillment_center_id,
    order_lines.fulfillment_channel
FROM {{ source('amazon', 'order_lines') }} AS order_lines
