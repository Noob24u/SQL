{{
    config(
        alias='contact_reason'
    )
}}

SELECT
    ticket_field_history.ticket_id,
    ticket_field_history.updated,
    ticket_field_history.value AS contact_reason_id,
    RANK() OVER (PARTITION BY ticket_field_history.ticket_id ORDER BY ticket_field_history.updated DESC) AS rank_reason
FROM fivetran.zendesk.ticket_field_history AS ticket_field_history
WHERE ticket_field_history.field_name = 'ticket_form_id'
QUALIFY rank_reason = 1
