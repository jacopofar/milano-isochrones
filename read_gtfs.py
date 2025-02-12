from pathlib import Path

import duckdb as ddb
import polars as pl

DECOMPRESSED_GTFS = Path('gtfs_data')

def get_metro_stops() -> pl.DataFrame:
    metro_lines = ddb.sql(f"""
    SELECT route_id
    FROM read_csv('{DECOMPRESSED_GTFS}/routes.txt')
    WHERE route_type = 1;
    """).pl()
    print("found metro lines:")
    print(metro_lines)
    # trips.txt maps a route with trip_id(s)
    # stop_times.txt maps a trip_id with a stop_id
    stops_in_lines = ddb.sql(f"""
    SELECT DISTINCT
        trips.route_id, stop_times.stop_id, stops.stop_lat, stops.stop_lon
    FROM read_csv('{DECOMPRESSED_GTFS}/trips.txt', types={{'shape_id': 'VARCHAR'}}) AS trips
    JOIN read_csv('{DECOMPRESSED_GTFS}/stop_times.txt', types={{'trip_id': 'VARCHAR', 'stop_id': 'VARCHAR'}}) AS stop_times
    ON trips.trip_id = stop_times.trip_id
    JOIN read_csv('{DECOMPRESSED_GTFS}/stops.txt', types={{'stop_id': 'VARCHAR', 'stop_lat': 'DOUBLE', 'stop_lon': 'DOUBLE'}}) AS stops
    ON stops.stop_id = stop_times.stop_id
    WHERE trips.route_id IN $route_ids;
    """, params=dict(route_ids=metro_lines['route_id'].to_list())).pl()
    return stops_in_lines
