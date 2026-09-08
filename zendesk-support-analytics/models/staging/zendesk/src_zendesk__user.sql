{{
    config(
        alias='user'
    )
}}

---- don't use the created_at FROM here.
SELECT
    user.id,
    user.url,
    user.name,
    user.email,
    CONVERT_TIMEZONE('Europe/Berlin', user.created_at)::TIMESTAMP_TZ AS created_at,
    CONVERT_TIMEZONE('Europe/Berlin', user.updated_at)::TIMESTAMP_TZ AS updated_at,
    user.time_zone,
    user.iana_time_zone,
    user.phone,
    user.locale,
    user.role,
    user.verified,
    user.authenticity_token,
    user.alias,
    user.active,
    user.shared,
    user.shared_agent,
    user.last_login_at,
    user.two_factor_auth_enabled,
    user.signature,
    user.details,
    user.notes,
    user.moderator,
    user.ticket_restriction,
    user.only_private_comments,
    user.restricted_agent,
    user.suspended,
    user.chat_only,
    user.report_csv,
    user.custom_followers_count,
    user.custom_instagram_account,
    user.custom_account_verified,
    user.locale_id,
    user.organization_id,
    user.external_id,
    user.custom_role_id,
    user.default_group_id,
    user.remote_photo_url
FROM fivetran.zendesk.user AS user
