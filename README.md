# SQL / Analytics Engineering

SQL and dbt work from my time running customer care, supply chain, and
pricing analytics for a multi-brand e-commerce company. Real production
code, lightly redacted where noted — see each project's own README for
details and known limitations.

## Projects

### [Zendesk Customer Care Analytics](https://github.com/Noob24u/SQL/blob/main/zendesk-support-analytics) (dbt)

A full dbt project (staging → intermediate → marts) that turns raw Zendesk
Support data into customer-care KPIs — first response time, handling time,
resolution time, contact rate, one-touch resolution — across two brands and
five countries. Includes a metric glossary, lineage diagram, documented bug
fixes, and a [sample KPI dashboard](https://claude.ai/code/artifact/28b75b5d-f27a-45af-b383-427a803f76eb)
built on synthetic data.

**Stack:** dbt, Snowflake, Fivetran.

### [Supply Chain Analytics](https://github.com/Noob24u/SQL/blob/main/supply-chain-analytics) (dbt)

A dbt project turning hand-maintained warehouse/planning spreadsheets into
governed stock and cost models — on-hand stock by SKU, landed cost per SKU
per month, inbound purchase orders, and a year-over-year stock value growth
metric — across two brands. Includes a documented rebuild of a broken raw
query into a working dbt model.

**Stack:** dbt, Snowflake, Fivetran (Google Sheets connector).

### [Pricing / Margin Analytics](https://github.com/Noob24u/SQL/blob/main/pricing-analytics) (dbt)

A dbt project computing actual per-SKU profitability on the Amazon
marketplace channel — net revenue minus landed cost, referral commission,
and FBA fulfilment fees, by SKU, country, and month. Builds on the
order-line cost logic used elsewhere in this repo, adding the aggregated
margin table that turns it into something a pricing decision can be based
on.

**Stack:** dbt, Snowflake, GCS ingest, Fivetran (Google Sheets connector).
