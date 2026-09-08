{{
    config(
        alias='user_details'
    )
}}

SELECT
    ticket.id,
    users.name,
    users.role,
    users.alias,
    users.active
FROM {{ref('src_zendesk__ticket')}} AS ticket
LEFT JOIN {{ref('src_zendesk__user')}} AS users
    ON ticket.submitter_id = users.id
GROUP BY 1, 2, 3, 4, 5
