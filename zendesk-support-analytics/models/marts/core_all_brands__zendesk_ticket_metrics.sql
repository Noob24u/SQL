{{
    config(
        alias='zendesk_ticket_metrics'
    )
}}

-- BUGFIX (see README "Bugs fixed while preparing this repo"): the assignee-name
-- lookup used to drive FROM the seed table (zendesk_assignee_names_seed) and
-- LEFT JOIN into int1_zendesk__assignee_details, with a COALESCE fallback to
-- the *string literal* 'names_seed.assignee_id' instead of the column. That
-- meant (a) any ticket whose assignee wasn't already present in the seed was
-- silently dropped from this CTE entirely, and (b) the fallback never worked
-- even when it should have. Fixed by driving FROM assignee_details (so every
-- ticket is preserved) and falling back to the real assignee_id column when
-- no human-readable name exists in the seed.
WITH assigee_name AS (
    SELECT
        assignee_details.ticket_id,
        COALESCE(names_seed.assignee_name, assignee_details.last_assigned) AS last_assignee_name
    FROM {{ref('int1_zendesk__assignee_details')}} AS assignee_details
    LEFT JOIN {{ref('zendesk_assignee_names_seed')}} AS names_seed
        ON assignee_details.last_assigned = names_seed.assignee_id
)

SELECT
    ticket.id,
    groups.brand,
    groups.country_name,
    ticket.via_channel,
    ticket.priority,
    ticket.status,
    ticket.created_at,
    ticket.updated_at,

    --- we would know from here on what was the first reply time
    first_reply_time.first_internal_comment_added,
    first_reply_time.first_external_comment_added,

    ticket.ticket_form_id,
    ticket.is_public,

    -- user details, proactive tickets
    ticket.submitter_id,
    user_details.name AS user_name,
    user_details.role AS user_role,
    user_details.alias AS user_alias,
    CASE WHEN user_details.role IN ('agent', 'admin')
            AND NOT ticket.via_channel = 'voice'
            AND NOT ticket.via_channel = 'chat'
         THEN 'True' ELSE 'False' END AS is_proactive_ticket,

    -- contact reason & secondary contact reason
    INITCAP(contact_reasons_combined.contact_reason) AS contact_reason,
    INITCAP(contact_reasons_combined.secondary_reason) AS secondary_reason,
    INITCAP(contact_reasons_combined.actions) AS actions,

    full_resolution_times.created_at_within_shift,
    full_resolution_times.full_resolution_time_in_minutes,
    full_resolution_times.full_resolution_time_in_sec,
    full_resolution_times.resolution_time_in_days,

    -- agent/assignee details
    ticket.assignee_id,
    assignee_details.last_assigned,
    INITCAP(assigee_name.last_assignee_name) AS last_assignee_name,
    assignee_details.solved_at,
    assignee_details.number_of_agents_assigned,
    reply_count_metrics.count_public_agent_comments,
    reply_count_metrics.count_agent_comments,
    reply_count_metrics.count_end_user_comments,
    reply_count_metrics.total_comments,
    reply_count_metrics.is_one_touch_resolution,
    reply_count_metrics.is_two_touch_resolution,
    ticket_handling_time.handling_time_in_sec
FROM {{ref('src_zendesk__ticket')}} AS ticket
LEFT JOIN {{ref('int1_zendesk__user_details')}} AS user_details
    ON ticket.id = user_details.id
LEFT JOIN {{ref('src_zendesk__groups')}} AS groups
    ON ticket.group_id = groups.id
LEFT JOIN {{ref('int1_zendesk__full_resolution_times')}} AS full_resolution_times
    ON ticket.id = full_resolution_times.ticket_id
LEFT JOIN {{ref('int1_zendesk__reply_count_metrics')}} AS reply_count_metrics
    ON ticket.id = reply_count_metrics.ticket_id
LEFT JOIN {{ref('int1_zendesk__ticket_handling_time')}} AS ticket_handling_time
    ON ticket.id = ticket_handling_time.ticket_id
LEFT JOIN {{ref('int1_zendesk__assignee_details')}} AS assignee_details
    ON ticket.id = assignee_details.ticket_id
LEFT JOIN {{ref('int1_zendesk__contact_reasons_combined')}} AS contact_reasons_combined
    ON ticket.id = contact_reasons_combined.ticket_id
LEFT JOIN {{ref('int1_zendesk__first_reply_time')}} AS first_reply_time
    ON ticket.id = first_reply_time.ticket_id
LEFT JOIN assigee_name
    ON assigee_name.ticket_id = ticket.id
WHERE NOT groups.brand = 'not_found'
    AND NOT groups.country_name = 'not_found'
