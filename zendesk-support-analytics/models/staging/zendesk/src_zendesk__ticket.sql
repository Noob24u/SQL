{{
    config(
        alias='ticket'
    )
}}

-- NOTE ON SCOPE: the real source table carries 90+ additional `custom_*`
-- columns beyond what's selected here -- one per Zendesk custom ticket field
-- (e.g. per-product-line claim reasons: candle component, diffuser component,
-- room spray component, eau de parfum component, discovery set component,
-- each with its own "_claim_reason" variant). They're a direct fingerprint of
-- the business this was built for, so this portfolio copy keeps only a
-- representative subset that downstream int1/core models actually reference,
-- plus a handful of custom_* fields to show the pattern. Nothing downstream
-- in this repo depends on the trimmed columns.
SELECT
    ticket.id,
    ticket.ticket_form_id,
    ticket.requester_id,
    ticket.submitter_id,
    ticket.assignee_id,
    ticket.organization_id,
    ticket.group_id,
    ticket.followup_ids,
    ticket.brand_id,
    ticket.forum_topic_id,
    ticket.problem_id,
    CONVERT_TIMEZONE('Europe/Berlin', ticket.created_at)::TIMESTAMP_TZ AS created_at,
    CONVERT_TIMEZONE('Europe/Berlin', ticket.updated_at)::TIMESTAMP_TZ AS updated_at,
    ticket.due_at,
    ticket.type,
    ticket.subject,
    ticket.description,
    ticket.priority,
    ticket.status,
    ticket.recipient,
    ticket.has_incidents,
    ticket.is_public,
    ticket.via_source_from_title,
    ticket.via_source_from_address,
    ticket.via_source_to_name,
    ticket.via_source_to_address,
    ticket.via_source_rel,
    ticket.via_channel,

    -- representative sample of the custom_* field pattern (see note above)
    ticket.custom_contact_reason,
    ticket.custom_action,
    ticket.custom_type_of_request,
    ticket.custom_country,
    ticket.custom_order_number,
    ticket.custom_order_status,
    ticket.custom_method_of_payment,
    ticket.custom_product_category,
    ticket.custom_reason_of_wrong_product,
    ticket.custom_reason_of_missing_item,
    ticket.custom_total_time_spent_sec_ AS custom_total_time_spent_sec,
    ticket.custom_time_spent_last_update_sec_ AS custom_time_spent_last_update_sec,

    ticket.allow_channelback,
    ticket.external_id,
    ticket.via_source_from_id,
    ticket.merged_ticket_ids,
    ticket.url
FROM fivetran.zendesk.ticket AS ticket
