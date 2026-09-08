{{
    config(
        alias='ticket_form_history'
    )
}}

SELECT
    ticket_form_history.id,
    CONVERT_TIMEZONE('Europe/Berlin', ticket_form_history.created_at)::TIMESTAMP_TZ AS created_at,
    CONVERT_TIMEZONE('Europe/Berlin', ticket_form_history.updated_at)::TIMESTAMP_TZ AS updated_at,
    ticket_form_history.name,
    ticket_form_history.display_name,
    ticket_form_history.end_user_visible,
    ticket_form_history.active,
    RANK() OVER (PARTITION BY ticket_form_history.id ORDER BY ticket_form_history.updated_at DESC) AS rank_name
FROM fivetran.zendesk.ticket_form_history AS ticket_form_history
QUALIFY rank_name = 1
