{{
    config(
        alias='new_open_ticket_daily'
    )
}}

WITH status_agg AS (
    SELECT
        ticket_field_history.ticket_id,
        ticket_field_history.updated_at,
        ticket_field_history.field_name,
        CASE WHEN ticket_field_history.field_name = 'status' THEN ticket_field_history.value END AS status,
        RANK()OVER (PARTITION BY ticket_field_history.ticket_id, ticket_field_history.updated_at::DATE ORDER BY ticket_field_history.updated_at ASC, ticket_field_history.field_name ASC) AS rank_status
    FROM {{ref('src_zendesk__ticket_field_history')}} AS ticket_field_history
    WHERE ticket_field_history.field_name = 'status'
    QUALIFY rank_status = 1
)

SELECT
    status_agg.ticket_id,
    status_agg.updated_at,
    status_agg.field_name,
    status_agg.status
FROM status_agg
WHERE status_agg.status IN ('new', 'open')
