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
),

-- Replace all values in the 'accessed_at' column with yesterday's timestamp which doesn't equal specific date
UPDATE uuid_generation
SET accessed_at = current_date() - 1
WHERE date_trunc('day', accessed_at) != '2024-10-14';

SELECT * FROM updated_accessed_at