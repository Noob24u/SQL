{{
    config(
        alias='day_light_saving'
    )
}}

---The daylight saving time starts on the last Sunday of March at 2:00 am (+1)
--- And ends in last Sunday of October at 3:00 am (puts back to 2:00 am)
--
-- NOTE: this implements the EU daylight-saving rule only (last Sunday of
-- March -> last Sunday of October). It was built to support timezone-correct
-- KPI calculations for the customer care team, which also handled US-brand
-- tickets on a different DST schedule (2nd Sunday of March -> 1st Sunday of
-- November). It is not currently referenced by
-- models/intermediate/int1_zendesk__full_resolution_times.sql, which still
-- uses a fixed Europe/Berlin business-hours window -- wiring this in (plus a
-- second US-rule version, parameterized by country/brand) is the natural next
-- step called out in the README's "Known limitations" section.
WITH start_date AS (
    SELECT
        days_seed.datum::DATE AS start_date,
        DAYNAME(days_seed.datum::DATE) AS day,
        YEAR(days_seed.datum) AS year,
        RANK() OVER (PARTITION BY DAYNAME(days_seed.datum::DATE), YEAR(days_seed.datum::DATE) ORDER BY (days_seed.datum::DATE) DESC, YEAR((days_seed.datum::DATE)) DESC) AS rank_day
    FROM {{ref('days_seed')}} AS days_seed
    WHERE MONTH(datum) = 3
        AND DAYNAME(datum) = 'Sun'
    GROUP BY 1, 2, 3
    QUALIFY rank_day = 1
),
end_date AS (
    SELECT
        days_seed.datum::DATE AS end_date,
        DAYNAME(days_seed.datum::DATE) AS day,
        YEAR(days_seed.datum) AS year,
        RANK() OVER (PARTITION BY DAYNAME(days_seed.datum::DATE), YEAR(days_seed.datum::DATE) ORDER BY (days_seed.datum::DATE) DESC, YEAR((days_seed.datum::DATE)) DESC) AS rank_day_end
    FROM {{ref('days_seed')}} AS days_seed
    WHERE MONTH(datum) = 10
        AND DAYNAME(datum) = 'Sun'
    GROUP BY 1, 2, 3
    QUALIFY rank_day_end = 1
)
, dst_agg As (
    SELECT
        start_date,
        end_date
    FROM start_date
    LEFT JOIN end_date
        ON start_date.year = end_date.year
)

SELECT
    days_seed.datum,
    CASE WHEN datum >= dst_agg.start_date AND datum <= dst_agg.end_date THEN 1
        ELSE 0 END AS is_dst
FROM  {{ref('days_seed')}} AS days_seed
LEFT JOIN dst_agg
    ON days_seed.datum >= dst_agg.start_date
    AND days_seed.datum <= dst_agg.end_date
