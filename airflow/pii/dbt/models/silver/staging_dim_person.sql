WITH source_data AS (
    SELECT
        unique_id,
        person_name,
        user_name,
        email,
        phone,
        birth_date,
        personal_number
    FROM
        {{ ref('raw_data') }}
)

SELECT
    *
FROM
    source_data
    