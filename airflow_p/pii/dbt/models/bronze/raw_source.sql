WITH
source_data AS (
    SELECT
        *
    FROM
        {{ source('csv_data', 'raw_data') }}
),
uuid_generation AS (
    SELECT
        uuid() AS unique_id,
        *
    FROM source_data
)

SELECT * FROM uuid_generation