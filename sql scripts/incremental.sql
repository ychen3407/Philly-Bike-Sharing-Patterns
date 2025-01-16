-- trip data fact table
CREATE OR REPLACE TABLE `pa_shared_bikes.fct_trip_data` AS
SELECT CAST(trip_id AS INT64) AS trip_id, 
        duration,
        start_time,
        end_time,
        start_station,
        end_station,
        bike_id,
        plan_duration,
        trip_route_category='Round Trip' AS is_round_trip,
        passholder_type,
        CASE WHEN bike_type='standard' THEN 'classic' ELSE bike_type END AS bike_type
FROM `pa_shared_bikes.stg_trip_data`



-- update dim_stations_scd table from stg_stations
MERGE INTO `station_project_test.dim_stations_scd` AS dim
USING (WITH
        dim_prev AS (SELECT * 
                FROM `station_project_test.dim_stations_scd` 
                WHERE end_date IS NULL)

        SELECT COALESCE(dim_prev.station_id,stg.station_id) AS station_id,
                COALESCE(stg.name, dim_prev.name) AS name,
                COALESCE(stg.lon, dim_prev.lon) AS lon,
                COALESCE(stg.lat, dim_prev.lat) AS lat,
                COALESCE(stg.station_type, dim_prev.station_type) AS station_type,
                COALESCE(stg.address, dim_prev.address) AS address,
                COALESCE(stg.active_status, FALSE) AS active_status,
                dim_prev.start_date AS start_date
        FROM dim_prev
        FULL OUTER JOIN `station_project_test.stg_stations` AS stg ON stg.station_id=dim_prev.station_id
) AS stg
ON dim.station_id=stg.station_id AND dim.active_status=stg.active_status AND dim.start_date=stg.start_date

-- CASE1: unchanged records
WHEN MATCHED THEN
        UPDATE SET
                dim.last_updated=CURRENT_TIMESTAMP()

-- CASE2: new records (brandly new + updated changed records)
WHEN NOT MATCHED BY TARGET THEN
  INSERT (station_id, name, lon, lat, station_type, address, active_status, start_date, end_date, last_updated)
    VALUES (
        stg.station_id,
        stg.name,
        stg.lon,
        stg.lat,
        stg.station_type,
        stg.address,
        stg.active_status,
        CURRENT_DATE(),
        NULL,
        CURRENT_TIMESTAMP()
    )

-- CASE3: outdate inactive records
WHEN NOT MATCHED BY SOURCE AND dim.end_date IS NULL THEN
        UPDATE SET
                dim.end_date=CURRENT_DATE(),
                dim.last_updated=CURRENT_TIMESTAMP()



-- increment fct_stations_status table on a daily basis
MERGE INTO `pa_shared_bikes.fct_stations_status` AS fct
USING (
  WITH
  deduped AS (
                SELECT *, DATETIME(utc_timestamp, 'America/New_York') AS est_datetime,
                      ROW_NUMBER() OVER (PARTITION BY station_id, utc_timestamp) AS r
                FROM `pa_shared_bikes.stations_status`
  )
  SELECT est_datetime, 
          station_id, 
          num_docks_available, 
          num_bikes_available, 
          num_bikes_available_electric, 
          num_bikes_available_smart, 
          num_bikes_available_classic
  FROM deduped
  WHERE r = 1 AND DATE(est_datetime) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AND CURRENT_DATE()
  ORDER BY est_datetime
) AS prev_ss
ON fct.station_id = prev_ss.station_id AND fct.est_datetime = prev_ss.est_datetime

WHEN NOT MATCHED THEN 
  INSERT (est_datetime, station_id, num_docks_available, num_bikes_available, num_bikes_available_electric, num_bikes_available_smart, num_bikes_available_classic)
  VALUES(
    prev_ss.est_datetime,
    prev_ss.station_id,
    prev_ss.num_docks_available,
    prev_ss.num_bikes_available,
    prev_ss.num_bikes_available_electric,
    prev_ss.num_bikes_available_smart,
    prev_ss.num_bikes_available_classic
  )
