{{
    config(
        alias='country_brand_name'
    )
}}

WITH ticket_id AS (
    SELECT
        try_cast(ticket_field_history.ticket_id  AS INTEGER) AS ticket_id,
        try_cast(ticket_field_history.value AS INTEGER) as group_id
    FROM {{ref('src_zendesk__ticket_field_history')}} AS ticket_field_history
    WHERE ticket_field_history.field_name = 'group_id'
)

SELECT
    ticket_id.ticket_id,
    try_cast(groups.id  AS INTEGER) AS id,
    groups.country_name,
    groups.brand
FROM {{ref('src_zendesk__groups')}} AS groups
LEFT JOIN ticket_id
    ON ticket_id.group_id = groups.id
