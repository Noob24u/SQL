{{
    config(
        alias='ticket_field_option'
    )
}}

SELECT
    ticket_field_option.value,
    ticket_field_option.name
FROM fivetran.zendesk.ticket_field_option AS ticket_field_option
