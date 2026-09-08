{{
    config(
        alias='ticket_field_history'
    )
}}

-- Zendesk logs a new row every time ANY tracked field changes on a ticket
-- (status, assignee, custom fields, ...). Every int1 model that needs "what
-- was this field at time X" or "when did this field last change" reads from
-- here rather than from ticket.sql, which only has the ticket's current state.
SELECT
    ticket_field_history.field_name,
    ticket_field_history.ticket_id,
    CONVERT_TIMEZONE('Europe/Berlin', ticket_field_history.updated)::TIMESTAMP_TZ AS updated_at,
    ticket_field_history.user_id,
    ticket_field_history.value
FROM fivetran.zendesk.ticket_field_history AS ticket_field_history
