{{
    config(
        alias='reply_count_metrics'
    )
}}

SELECT
    ticket_comment.ticket_id,
    SUM(CASE WHEN(user.role) IN ('agent','admin') AND ticket_comment.public = true
        THEN 1 ELSE 0 END) AS count_public_agent_comments,
    SUM(CASE WHEN(user.role) IN ('agent','admin')
        THEN 1 ELSE 0 END) AS count_agent_comments,
    SUM(CASE WHEN(user.role) = 'end-user'
        THEN 1 ELSE 0 END) AS count_end_user_comments,
    COUNT(*) AS total_comments,
    SUM(CASE WHEN(user.role) IN ('agent','admin') AND ticket_comment.public = true
        THEN 1 ELSE 0 END) < 2 AS is_one_touch_resolution,
    SUM(CASE WHEN(user.role) IN ('agent','admin') AND ticket_comment.public = true
        THEN 1 ELSE 0 END) = 2 AS is_two_touch_resolution
FROM {{ref('src_zendesk__user')}} AS user
LEFT JOIN {{ref('src_zendesk__ticket_comment')}} AS ticket_comment
    ON user.id = ticket_comment.user_id
GROUP BY 1
