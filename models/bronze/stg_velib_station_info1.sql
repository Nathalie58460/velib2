{{ config(
    materialized = 'table',
    tags = ['velib', 'staging']
) }}

WITH src AS (
    SELECT
        PAYLOAD
    FROM {{ source('velib', 'station_information_raw') }}
),

stations AS (
    SELECT
        f.value:station_id::number      AS station_id,
        f.value:name::string            AS station_name,
        f.value:lat::float              AS latitude,
        f.value:lon::float              AS longitude,
        f.value:capacity::number        AS capacity,
        CURRENT_TIMESTAMP()             AS load_timestamp
    FROM src,
        LATERAL FLATTEN(input => src.PAYLOAD:data:stations) f
)

SELECT *
FROM stations;