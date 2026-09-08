{{
    config(
        alias='reason_for_contact'
    )
}}

SELECT
    ticket.id,
    ticket.ticket_form_id,
    listagg(distinct ticket_form_history.name, ', ') AS contact_reason
FROM {{ref('src_zendesk__ticket_form_history')}} AS ticket_form_history
LEFT JOIN {{ref('src_zendesk__ticket')}}  AS ticket
    ON ticket_form_history.id = ticket.ticket_form_id
GROUP BY 1, 2
