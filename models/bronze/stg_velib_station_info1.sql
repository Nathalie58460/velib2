SELECT
    f.value:station_id::number      AS station_id,
    f.value:name::string            AS name,
    f.value:lat::float              AS latitude,
    f.value:lon::float              AS longitude,
    f.value:capacity::number        AS capacity
FROM @VELIB_STAGE/station_information.json
    (file_format => JSON_VELIB),
    LATERAL FLATTEN(input => $1:data:stations) f