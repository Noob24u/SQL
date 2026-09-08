{{
    config(
        alias='actions'
    )
}}

WITH actions_agg AS (
    SELECT
        actions.ticket_id,
        actions.updated,
        actions.id,
        ticket_field_option.name AS actions,
        RANK() OVER (PARTITION BY actions.ticket_id ORDER BY actions.updated DESC) AS rank_reason
    FROM {{ref('src_zendesk__ticket_field_option')}} AS ticket_field_option
    LEFT JOIN {{ref('src_zendesk__actions')}} AS actions
        ON ticket_field_option.value = actions.actions
    QUALIFY rank_reason = 1
)

SELECT
    ticket.id,
    actions_agg.id AS action_id,
    actions_agg.updated AS action_updated_at,
    TRIM(actions_agg.actions) AS actions
FROM {{ref('src_zendesk__ticket')}} AS ticket
LEFT JOIN actions_agg
    ON ticket.id = actions_agg.ticket_id
