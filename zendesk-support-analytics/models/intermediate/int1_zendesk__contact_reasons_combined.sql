{{
    config(
        alias='contact_reasons_combined'
    )
}}

SELECT
    contact_reason.ticket_id,
    contact_reason.contact_reason_id,
    secondary_contact_reason.secondary_reason_id,
    actions.action_id,
    contact_reason.contact_reason,
    CASE WHEN contact_reason.contact_reason_id IN ( '360001048999', '360001068300', '360001250880')
         THEN NULL ELSE secondary_contact_reason.secondary_reason
         END AS secondary_reason,
    CASE WHEN contact_reason.contact_reason_id IN ('360001068560', '360001049019', '360001228879', '360001250900', '360001049219')
         THEN NULL ELSE actions.actions
         END AS actions
FROM {{ref('int1_zendesk__contact_reason')}} AS contact_reason
LEFT JOIN {{ref('int1_zendesk__secondary_contact_reason')}} AS secondary_contact_reason
    ON contact_reason.ticket_id = secondary_contact_reason.id
LEFT JOIN {{ref('int1_zendesk__actions')}} AS actions
    ON contact_reason.ticket_id = actions.id
