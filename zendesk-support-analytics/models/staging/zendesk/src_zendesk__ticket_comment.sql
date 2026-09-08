{{
    config(
        alias='ticket_comment'
    )
}}

SELECT
    ticket_comment.id,
    ticket_comment.body,
    ticket_comment.ticket_id,
    ticket_comment.facebook_comment,
    ticket_comment.voice_comment,
    ticket_comment.tweet,
    CONVERT_TIMEZONE('Europe/Berlin', ticket_comment.created)::TIMESTAMP_TZ AS created_at,
    ticket_comment.user_id,
    ticket_comment.public,
    ticket_comment.voice_comment_transcription_visible,
    ticket_comment.trusted,
    ticket_comment.transcription_status,
    ticket_comment.started_at,
    ticket_comment.location,
    ticket_comment.call_duration,
    ticket_comment.call_id,
    ticket_comment.transcription_text,
    ticket_comment.recording_url
FROM fivetran.zendesk.ticket_comment AS ticket_comment
