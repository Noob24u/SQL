{{
    config(
        alias='secondary_reason'
    )
}}

SELECT
    ticket_field_history.ticket_id,
    ticket_field_history.updated,
    ticket_field_history.field_name AS secondary_reason_id,
    ticket_field_history.value AS secondary_reason
FROM fivetran.zendesk.ticket_field_history AS ticket_field_history
WHERE ticket_field_history.field_name IN (
    '360014672660', '360014671940', '360023676639', '360014640139', '360024921860', '360020670319', '360015073580',
    '360014890900', '360015073500', '360015073600', '360020670359', '360020549480', '360016516600', '360021233280'
)
