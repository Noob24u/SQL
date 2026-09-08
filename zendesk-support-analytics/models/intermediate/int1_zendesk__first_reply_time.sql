{{
    config(
        alias='first_reply_time'
    )
}}

-- In this model, we would calculate first comment by external(end-user) and internal(agent/admin) commenter
-- commenter role is created. this would help to decide if it were an internal/external comment
WITH first_commenter_role AS (
    SELECT
        ticket_comment.ticket_id,
        ticket_comment.created_at,
        ticket_comment.user_id,
        ticket_comment.body AS comments,
        user.role,
        CASE WHEN user.role = 'end-user' THEN 'external_comment'
             WHEN user.role IN ('agent', 'admin') THEN 'internal_comment'
             ELSE 'unknown' END AS commenter_role
    FROM {{ref('src_zendesk__user')}} AS user
    LEFT JOIN {{ref('src_zendesk__ticket_comment')}} AS ticket_comment
        ON user.id = ticket_comment.user_id
    GROUP BY 1, 2, 3, 4, 5, 6
),

-- ranking based on commenter would tell us when was the first comment added by each commenter
rank_commenters AS (
    SELECT
        first_commenter_role.ticket_id,
        first_commenter_role.created_at,
        first_commenter_role.user_id,
        first_commenter_role.role,
        first_commenter_role.commenter_role,
        RANK() OVER (PARTITION BY first_commenter_role.ticket_id, first_commenter_role.role ORDER BY first_commenter_role.created_at DESC) AS rank_ticket
    FROM first_commenter_role
    GROUP BY 1, 2, 3, 4, 5
)
-- since proactive tickets should not be counted, we would calculate 1st reply time in the core.
-- because in core, we have proactive tickets data

SELECT
    rank_commenters.ticket_id,
    MIN(CASE WHEN rank_commenters.rank_ticket = 1 AND rank_commenters.commenter_role = 'internal_comment'
             THEN rank_commenters.created_at END) AS first_internal_comment_added,
    MIN(CASE WHEN rank_commenters.rank_ticket = 1 AND rank_commenters.commenter_role = 'external_comment'
             THEN rank_commenters.created_at END) AS first_external_comment_added
FROM rank_commenters
GROUP BY 1
