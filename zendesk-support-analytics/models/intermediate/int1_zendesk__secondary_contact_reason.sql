{{
    config(
        alias='secondary_contact_reason'
    )
}}

WITH secondary_contact_reason_rank AS (
    SELECT
        secondary_contact_reason.ticket_id,
        secondary_contact_reason.updated,
        secondary_contact_reason.secondary_reason_id,
        ticket_field_option.name AS secondary_reason,
        RANK() OVER (PARTITION BY secondary_contact_reason.ticket_id ORDER BY secondary_contact_reason.updated DESC) AS rank_reason
    FROM {{ref('src_zendesk__ticket_field_option')}} AS ticket_field_option
    LEFT JOIN {{ref('src_zendesk__secondary_reason')}} AS secondary_contact_reason
        ON ticket_field_option.value = secondary_contact_reason.secondary_reason
    QUALIFY rank_reason = 1
)

SELECT
    ticket.id,
    secondary_contact_reason_rank.secondary_reason_id,
    secondary_contact_reason_rank.updated,
    TRIM(secondary_contact_reason_rank.secondary_reason) AS secondary_reason
FROM {{ref('src_zendesk__ticket')}} AS ticket
LEFT JOIN secondary_contact_reason_rank
    ON ticket.id = secondary_contact_reason_rank.ticket_id
