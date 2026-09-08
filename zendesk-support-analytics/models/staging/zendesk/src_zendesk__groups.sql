{{
    config(
        alias='groups'
    )
}}

-- Maps Zendesk "group" (a support queue) to the brand + country it serves.
-- Group names in Zendesk follow a "Brand_Country" (or "Brand_ Country") naming
-- convention set up by the support team; this parses that convention rather
-- than relying on a separate lookup table.
SELECT
    groups.id,
    groups.name,
    CASE WHEN groups.name ILIKE LOWER('Ava & May_%') THEN 'am'
        WHEN groups.name ILIKE LOWER('Marketing%') THEN 'not_found'
        WHEN groups.name ILIKE LOWER('Million Facets%') THEN 'not_found'
        WHEN groups.name ILIKE LOWER('faynt%') THEN 'faynt'
        WHEN groups.name ILIKE LOWER('%General') THEN 'not_found'
        ELSE groups.name END AS brand,
    CASE WHEN groups.name ILIKE LOWER('Ava & May_%') THEN regexp_substr(groups.name, '_+(\\w+)', 1, 1, 'e', 1)
        WHEN groups.name ILIKE LOWER('Marketing%') THEN regexp_substr(groups.name, ' +(\\w+)', 1, 1, 'e', 1)
        WHEN groups.name ILIKE LOWER('Million Facets%') THEN regexp_substr(groups.name, '_+(\\w+)', 1, 1, 'e', 1)
        WHEN groups.name ILIKE LOWER('faynt%') THEN regexp_substr(groups.name, '_+(\\w+)', 1, 1, 'e', 1)
        WHEN groups.name ILIKE LOWER('%General') THEN 'not_found'
        ELSE groups.name END AS country_name,
    CONVERT_TIMEZONE('Europe/Berlin', groups.created_at)::TIMESTAMP_TZ AS created_at,
    CONVERT_TIMEZONE('Europe/Berlin', groups.updated_at)::TIMESTAMP_TZ AS updated_at,
    groups.url
FROM fivetran.zendesk."GROUP" AS groups
