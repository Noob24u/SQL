# Pricing / Margin Analytics (dbt)

A dbt project that turns Amazon marketplace order data into an actual
per-SKU profitability number: net revenue minus landed product cost, Amazon
referral commission, and FBA fulfilment fees, by SKU, country, and month.

## Background

This is real production dbt code from the same role as the
[Zendesk](../zendesk-support-analytics/) and
[Supply Chain](../supply-chain-analytics/) projects in this repo. Amazon
was one of several sales channels; `core_amazon__order_lines` already
computed commission, landed cost, and FBA fee per order line in the
original codebase — this project rebuilds that model cleanly (PII
removed, seed dependency made explicit and synthetic) and adds the piece
that didn't exist yet: an actual aggregated margin table
(`core_pricing__amazon_sku_margin`) that answers "is this SKU actually
profitable on Amazon" instead of leaving that as a manual spreadsheet
exercise on top of the order-line detail.

Brand names are anonymized as `brand1` 

## Problem this solves

`core_amazon__order_lines` already had all three cost components
(commission, landed cost, FBA fee) computed at the line-item level, but
nothing aggregated them into a number a pricing decision could actually be
based on — "should we drop this SKU's price" or "is this SKU worth
keeping on Amazon at all" required someone to pull the line-item table
into a spreadsheet and do the math by hand, SKU by SKU. `core_pricing__amazon_sku_margin`
is that aggregation, done once, governed, and queryable directly.

## Architecture

```mermaid
flowchart LR
    subgraph Source
        RAW1[("gcs.amazon.order_lines")]
        RAW2[("fivetran.gsheets.product_cost_amazon\n/ product_cost_brand1")]
        SEED[("amazon_fba_fees_seed\n(synthetic)")]
    end

    subgraph Staging["staging (src_*__*)"]
        S1[order_lines]
        S2[variant_product_costs_amazon]
        S3[variant_product_costs_brand1]
    end

    subgraph Core["core"]
        M1[core_amazon__variant_product_costs]
        M2[core_brand1__variant_product_costs]
        M3[core_amazon__order_lines]
        M4[[core_pricing__amazon_sku_margin]]
    end

    RAW1 --> S1
    RAW2 --> S2 & S3
    S1 --> M3
    S2 --> M1
    S3 --> M2
    M1 & M2 --> M3
    SEED --> M3
    M3 --> M4
```

- **staging (`models/staging/pricing`)** — light typing/renaming, plus the
  same product-cost unpivot pattern used in Supply Chain Analytics.
- **core (`models/core`)** — `core_amazon__order_lines` is line-item grain
  with cost/fee components computed; `core_pricing__amazon_sku_margin` is
  the aggregated, queryable margin table.

Warehouse: **Snowflake** (`DIV0`, `DATE_TRUNC`, `LATERAL FLATTEN`,
`OBJECT_CONSTRUCT_KEEP_NULL` throughout).

## Metric glossary

| Metric | Definition | Model |
|---|---|---|
| Net revenue | `paid_amount_net` summed per SKU/country/month — item price net of tax, plus discounts, shipping, and gift wrap, excluding tax | `core_pricing__amazon_sku_margin` |
| Product cost | Landed cost per unit (Amazon-channel cost sheet + brand1 base cost sheet, summed) × units shipped | `core_amazon__order_lines`, aggregated in the margin model |
| Commission cost | `paid_amount_net x 0.15` — a flat assumed Amazon referral commission rate | `core_amazon__order_lines` |
| FBA fee | Expected domestic fulfilment fee per unit from the FBA fee seed, converted to EUR for PLN-denominated rows, × units shipped | `core_amazon__order_lines` |
| Contribution margin | `net_revenue - product_cost - commission_cost - fba_fee` | `core_pricing__amazon_sku_margin` |
| Contribution margin % | `contribution_margin / net_revenue`, DIV0-guarded | `core_pricing__amazon_sku_margin` |

Full column-level descriptions live in `models/core/_core_pricing__models.yml`.

## Known limitations

- **The 15% commission rate is hardcoded**
- **The PLN→EUR conversion in the FBA fee calculation uses a hardcoded
  0.23 rate**, and only handles PLN
- **No logistics cost allocation.** `core_amazon__logistic_costs` (freight,
  palletizing) exists in the Supply Chain project but isn't joined in here
- **`amazon_fba_fees_seed.csv` is fully synthetic** 

## What was changed for this repo

- Buyer PII (`buyer_email`, `buyer_name`, `buyer_phone_number`) and all
  ship-to/bill-to address fields are dropped entirely at the staging
  layer — not just unused downstream, not selected at all.
- `amazon_fba_fees_seed.csv` is synthetic sample data (three example SKUs,
  a few countries/currencies) — the original seed with real fee data isn't
  included.
- A dead no-op (`... + order_lines.shipment_discount_amount * 1 AS
  total_discount`) in the original `core_amazon__order_lines` is cleaned
  up to drop the `* 1` — same value, no behavior change.
- Brand name anonymized to `brand1`, consistent with the Supply Chain
  Analytics project.
- `core_pricing__amazon_sku_margin` is new — it didn't exist in the
  original codebase. This is the actual "pricing decision" deliverable;
  everything else here is the (real, pre-existing) data it's built from.

## Running this project

This project reads from a Snowflake warehouse populated by a GCS-based
Amazon ingest job and Fivetran's Google Sheets connector — it isn't
runnable standalone without those sources. 
