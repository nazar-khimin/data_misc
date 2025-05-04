WITH source_data AS (
    SELECT
        unique_id,
        address,
        mac_address,
        ip_address
    FROM
        {{ ref('raw_data') }}
)

SELECT
    *
FROM
    source_data
    