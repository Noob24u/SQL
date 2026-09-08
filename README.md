# SQL / Analytics Engineering

SQL and dbt work from my time running customer care and supply chain
analytics for a multi-brand e-commerce company. Real production code,
lightly redacted where noted — see each project's own README for details
and known limitations.

## Projects

### [Zendesk Customer Care Analytics](zendesk-support-analytics/) (dbt)

A full dbt project (staging → intermediate → marts) that turns raw Zendesk
Support data into customer-care KPIs — first response time, handling time,
resolution time, contact rate, one-touch resolution — across two brands and
five countries. Includes a metric glossary, lineage diagram, documented bug
fixes, and a [sample KPI dashboard](https://claude.ai/code/artifact/28b75b5d-f27a-45af-b383-427a803f76eb)
built on synthetic data.

**Stack:** dbt, Snowflake, Fivetran.

### [`inventory_growth.sql`](inventory_growth.sql)

A supply-chain query calculating week-over-week and year-over-year stock
value growth by brand/SKU, from weekly stock snapshots and product cost
data.
