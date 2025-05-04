WITH source_data AS (
    SELECT
        unique_id,
        accessed_at
    FROM
        {{ ref('raw_data') }}
)

SELECT
    *
FROM
    source_data
    