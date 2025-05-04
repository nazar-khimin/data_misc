WITH source_data AS (
    SELECT
        unique_id,
        session_duration,
        download_speed,
        upload_speed,
        consumed_traffic
    FROM
        {{ ref('raw_data') }}
)

SELECT
    *
FROM
    source_data
    