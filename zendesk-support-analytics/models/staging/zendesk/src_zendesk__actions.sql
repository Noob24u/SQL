{{
    config(
        alias='actions'
    )
}}

SELECT
    ticket_field_history.ticket_id,
    ticket_field_history.updated,
    ticket_field_history.field_name AS id,
    ticket_field_history.value AS actions
FROM fivetran.zendesk.ticket_field_history AS ticket_field_history
WHERE ticket_field_history.field_name IN (
    '360014672640','360014642559','360014642399','360020549460', '360016515860'
)
