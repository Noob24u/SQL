{{
    config(
        alias='ticket_handling_time'
    )
}}
--- handling time in round numbers
--- This is for Handling time. For how long an agent has worked ON the ticket

-- '360000000000' is a placeholder standing in for the real Zendesk custom
-- field ID (anonymized for this portfolio repo). In production Zendesk, this
-- field is a numeric "time tracking" app field that logs cumulative seconds
-- an agent has had the ticket open/actively worked, written as a new
-- ticket_field_history row on every update.
SELECT
    ticket_field_history.ticket_id,
    ROUND(SUM(ticket_field_history.value), 0) AS handling_time_in_sec
FROM {{ref('src_zendesk__ticket_field_history')}} AS ticket_field_history
WHERE field_name = '360000000000'
GROUP BY 1
