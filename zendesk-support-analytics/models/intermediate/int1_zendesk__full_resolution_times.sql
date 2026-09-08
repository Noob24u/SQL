{{
    config(
        alias='full_resolution_times'
    )
}}

--- calculate resolution time
--- KNOWN LIMITATION: business hours are hardcoded as Mon-Fri 07:00-18:00 Europe/Berlin
--- (weekends excluded). It does not currently adjust for daylight saving time or for
--- agents/customers in other timezones (e.g. US brands) -- see models/utils/utils__day_light_saving.sql
--- for a DST-aware date flag that was built separately but is not yet wired into this model.

WITH ticket_solved_time AS (
    SELECT
        ticket.id AS ticket_id,
        ticket.created_at AS original_ticket_created_at,
        DAYNAME(ticket.created_at::DATE) AS day,
        TO_TIME(ticket.created_at) as hour,
        CASE
            WHEN DAYNAME(ticket.created_at::DATE) IN ('Mon', 'Tue', 'Wed', 'Thu', 'Fri') and TO_TIME(ticket.created_at) >= '7:00:00' AND TO_TIME(ticket.created_at) < '18:00:00' then ticket.created_at -- takes care of time
            WHEN DAYNAME(ticket.created_at::DATE) NOT IN ('Sat', 'Sun', 'Fri') AND TO_TIME(ticket.created_at) > '18:00:00' AND TO_TIME(ticket.created_at) <= '23:59:59' Then  CONCAT(DATE(ticket.created_at) + 1, ' ',  '7:00:00')::TIMESTAMP
            WHEN DAYNAME(ticket.created_at::DATE) NOT IN ('Sat', 'Sun') AND TO_TIME(ticket.created_at) > '00:00:00' AND  TO_TIME(ticket.created_at) < '07:00:00' Then CONCAT(DATE(ticket.created_at), ' ',  '7:00:00')::TIMESTAMP
            WHEN DAYNAME(ticket.created_at::DATE) IN ('Fri') AND TO_TIME(ticket.created_at) > '18:00:00' AND TO_TIME(ticket.created_at) <= '23:59:59' Then  CONCAT(DATE(ticket.created_at) + 3, ' ',  '7:00:00')::TIMESTAMP
            WHEN DAYNAME(ticket.created_at::DATE) IN ('Sat') Then  CONCAT(DATE(ticket.created_at)+ 2, ' ',  '7:00:00')::TIMESTAMP
            WHEN DAYNAME(ticket.created_at::DATE) IN ('Sun') Then  CONCAT(DATE(ticket.created_at)+ 1, ' ',  '7:00:00')::TIMESTAMP
        END AS created_at_within_shift
    FROM {{ref('src_zendesk__ticket')}}  AS ticket
)
,
full_resolution_time_in_minutes AS (
    SELECT
        ticket_solved_time.ticket_id,
        ticket_solved_time.original_ticket_created_at,
        ticket_solved_time.day,
        ticket_solved_time.created_at_within_shift,
        DAYNAME(ticket_solved_time.created_at_within_shift) AS day_shift,
        MAX(CASE WHEN ticket_field_history.value = 'solved' THEN ticket_field_history.updated_at END) AS solved_at, -- tkt id : 96677, 2021-01-10 20:01:30.000
        DATEDIFF(minute ,DATE_TRUNC('week', ticket_solved_time.created_at_within_shift), ticket_solved_time.created_at_within_shift) + 1440 AS start_time_in_minutes_from_week,
        DATEDIFF(minute,ticket_solved_time.created_at_within_shift, MAX(CASE WHEN ticket_field_history.value = 'solved' THEN ticket_field_history.updated_at END)) AS full_resolution_time_in_minutes
    FROM ticket_solved_time
    LEFT JOIN {{ref('src_zendesk__ticket_field_history')}} AS ticket_field_history
    ON ticket_solved_time.ticket_id = ticket_field_history.ticket_id
    GROUP BY 1, 2, 3, 4
)
SELECT
    full_resolution_time_in_minutes.ticket_id,
    full_resolution_time_in_minutes.original_ticket_created_at,
    full_resolution_time_in_minutes.day,
    full_resolution_time_in_minutes.created_at_within_shift,
    full_resolution_time_in_minutes.day_shift,
    full_resolution_time_in_minutes.solved_at,
    full_resolution_time_in_minutes.full_resolution_time_in_minutes,
    full_resolution_time_in_minutes.full_resolution_time_in_minutes * 60 AS full_resolution_time_in_sec,
    CASE WHEN full_resolution_time_in_minutes.full_resolution_time_in_minutes >= 1400 THEN DIV0(full_resolution_time_in_minutes.full_resolution_time_in_minutes,1400) ELSE 0  END AS resolution_time_in_days
FROM full_resolution_time_in_minutes
