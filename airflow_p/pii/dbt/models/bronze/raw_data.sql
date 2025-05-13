WITH
accessed_at_update AS (
    SELECT
        * EXCLUDE (accessed_at),
        CASE
            WHEN date_trunc('day', accessed_at) != DATE '2024-10-14'
            THEN current_date() - 1
            ELSE accessed_at
        END AS accessed_at
    FROM {{ ref('raw_source') }}
)

SELECT * FROM accessed_at_update