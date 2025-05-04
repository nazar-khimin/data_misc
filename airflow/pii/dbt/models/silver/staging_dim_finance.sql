WITH source_data AS (
    SELECT
        unique_id,
        iban
    FROM
        {{ ref('raw_data') }}
)

SELECT
    *
FROM
    source_data
    