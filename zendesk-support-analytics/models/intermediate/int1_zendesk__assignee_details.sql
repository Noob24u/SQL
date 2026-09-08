{{
    config(
        alias='assignee_details'
    )
}}

--- assignee id information. WHERE are their names?
--- is it common to have multiple assignees/agents in one ticket?
WITH rank_assignees AS (
    SELECT
        ticket_field_history.ticket_id,
        ticket_field_history.updated_at AS assignee_last_updated_at,
        ticket_field_history.value AS assignee_id,
        RANK() OVER ( PARTITION BY ticket_field_history.ticket_id ORDER BY ticket_field_history.updated_at  DESC ) AS rank_assignee
    FROM {{ref('src_zendesk__ticket_field_history')}} AS ticket_field_history
    WHERE ticket_field_history.field_name = 'assignee_id'
    GROUP BY 1, 2, 3
),

solved_at AS (
    SELECT
        ticket_field_history.ticket_id,
        MAX(ticket_field_history.updated_at) AS solved_at
    FROM {{ref('src_zendesk__ticket_field_history')}} AS ticket_field_history
    WHERE ticket_field_history.value = 'solved'
    GROUP BY 1
)

SELECT
    rank_assignees.ticket_id,
    solved_at.solved_at,
    MIN(CASE WHEN rank_assignees.rank_assignee = 1 THEN rank_assignees.assignee_id END) AS last_assigned,
    COUNT(DISTINCT rank_assignees.assignee_id) AS number_of_agents_assigned
FROM rank_assignees
LEFT JOIN solved_at
    ON solved_at.ticket_id = rank_assignees.ticket_id
GROUP BY 1, 2
