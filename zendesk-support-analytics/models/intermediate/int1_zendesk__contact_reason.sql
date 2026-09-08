{{
    config(
        alias='contact_reason'
    )
}}

SELECT
    contact_reason.ticket_id,
    contact_reason.contact_reason_id,
    contact_reason.updated,
    INITCAP(TRIM(ticket_form_history.name)) AS contact_reason
FROM {{ref('src_zendesk__ticket_form_history')}} AS ticket_form_history
LEFT JOIN {{ref('src_zendesk__contact_reason')}} AS contact_reason
    ON ticket_form_history.id = contact_reason.contact_reason_id
