import datetime
import json
import logging
import warnings

import geopandas as gpd
import pandas as pd
import r5py
from shapely import Point
from r5py import TransportNetwork
import numpy as np
import duckdb as ddb

from read_gtfs import get_metro_stops


logger = logging.getLogger()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
REFERENCE_TIME = datetime.datetime(2025, 2, 6, 15, 10)

# with 0.002 is 22m
# with 0.0005 expected 6h
RESOLUTION = 0.0005
# BBox around Milan:
MINX, MAXX = 9.047, 9.334
MINY, MAXY = 45.3803, 45.5614


def get_travel_itinerary(
    transport_network: TransportNetwork, origin: Point, destination: Point
) -> None:
    """Calculates the itineraries between two points, stores them as GeoJSON"""
    origins = gpd.GeoDataFrame(
        {"id": [1], "geometry": [origin]},
        crs="EPSG:4326",
    )

    destinations = gpd.GeoDataFrame(
        {"id": [1], "geometry": [destination]},
        crs="EPSG:4326",
    )

    detailed_itineraries_computer = r5py.DetailedItinerariesComputer(
        transport_network,
        origins=origins,
        destinations=destinations,
        departure=REFERENCE_TIME,
        transport_modes=[r5py.TransportMode.TRANSIT, r5py.TransportMode.WALK],
        snap_to_network=True,
    )

    travel_details = detailed_itineraries_computer.compute_travel_details()
    logger.info(travel_details)
    # travel_details.dtypes
    # from_id                     int64
    # to_id                       int64
    # option                      int64
    # segment                     int64
    # transport_mode             object
    # departure_time     datetime64[ns]
    # distance                  float64
    # travel_time       timedelta64[ns]
    # wait_time         timedelta64[ns]
    # route                      object
    # geometry                 geometry

    # this is to make th dataframe palatable to JSON
    # store the name of the transport mode, not the object itself (which would be TransportMode.WALK instead of 'walk')

    # with open("travel_details.json", "w") as wf:
    #     wf.write(travel_details.to_json())

    options_totals = (
        travel_details[["option", "travel_time", "wait_time"]]
        .groupby("option")
        .sum()
        .copy()
    )
    options_totals["total_time"] = (
        options_totals["travel_time"] + options_totals["wait_time"]
    )
    fastest_id = options_totals["total_time"].idxmin()
    logger.info("fastest option: ", fastest_id)
    for route_option in travel_details["option"].unique():
        logger.info(f"Writing option {route_option}")
        this_route = travel_details.query("option == @route_option").copy()
        total_time = this_route["travel_time"].sum() + this_route["wait_time"].sum()
        transport_modes = (
            this_route["transport_mode"].apply(lambda tm: tm.name).unique()
        )
        logger.info(
            f"This route will take {total_time} using transport modes {transport_modes}"
        )

        with open(f"single_routes/route_{route_option}.json", "w") as wf:
            this_route["transport_mode"] = this_route["transport_mode"].apply(
                lambda tm: tm.name
            )
            this_route["departure_time"] = this_route["departure_time"].apply(
                lambda tt: tt.isoformat()
            )
            this_route["travel_time"] = this_route["travel_time"].apply(str)
            this_route["wait_time"] = this_route["wait_time"].apply(str)
            wf.write(this_route.to_json())
        if route_option == fastest_id:
            with open("single_routes/route_fastest.json", "w") as wf:
                wf.write(this_route.to_json())


def get_cells() -> dict[str, Point]:
    """Get a dictionary of reference points, for each a key and a Point.

    The Point is an EPSG4326 coordinate
    """
    cells: dict[str, Point] = dict()
    for idx_x, x in enumerate(np.arange(MINX, MAXX, RESOLUTION)):
        for idx_y, y in enumerate(np.arange(MINY, MAXY, RESOLUTION)):
            cell_center = Point(x + RESOLUTION / 2, y + RESOLUTION / 2)
            cells[f"{idx_x},{idx_y}"] = cell_center
    return cells


if __name__ == "__main__":
    transport_network = TransportNetwork.from_directory("input_data")
    cells = get_cells()
    metro_stops = get_metro_stops()
    destinations = gpd.GeoDataFrame(
        {
            "id": [
                f"{sid} ({rid})"
                for sid, rid in zip(
                    metro_stops["stop_id"].to_list(),
                    metro_stops["route_id"].to_list(),
                )
            ],
            "geometry": [
                Point(lon, lat)
                for lat, lon in zip(
                    metro_stops["stop_lat"].to_list(), metro_stops["stop_lon"].to_list()
                )
            ],
        },
        crs="EPSG:4326",
    )
    origins = gpd.GeoDataFrame(
        {
            "id": cells.keys(),
            "geometry": cells.values(),
        },
        crs="EPSG:4326",
    )
    logger.info(f"Sample origin points: {len(origins)}")

    CHUNK_SIZE = 5000
    travel_times_dfs = []
    # TODO this is not working with action="once", why?
    # necessary because of the warning:
    # RuntimeWarning: Departure time 2025-02-06 15:10:00 is outside of the
    # time range covered by currently loaded GTFS data sets.
    #
    # However the date is in the range, and the results look fine
    with warnings.catch_warnings(action="ignore"):
        for i in range(0, len(origins), CHUNK_SIZE):
            logger.info(f"Processing origins {i}-{i + CHUNK_SIZE}")
            travel_time_matrix = r5py.TravelTimeMatrixComputer(
                transport_network,
                origins=origins[i : i + CHUNK_SIZE],
                destinations=destinations,
                transport_modes=[
                    r5py.TransportMode.WALK,
                    r5py.TransportMode.BUS,
                    r5py.TransportMode.TRAM,
                ],
                departure=REFERENCE_TIME,
            ).compute_travel_times()
            travel_time_matrix.to_pickle(f"distances_cache/{i}.pkl")
            travel_times_dfs.append(travel_time_matrix)
    travel_time_matrix = pd.concat(travel_times_dfs, ignore_index=True)
    travel_time_matrix.to_pickle("times.pkl")
    # travel_time_matrix = pickle.load(open("times.pkl", "rb"))
    travel_time_matrix = travel_time_matrix.dropna(subset=["travel_time"])
    nearest_stations = travel_time_matrix.loc[
        travel_time_matrix.groupby("from_id")["travel_time"].idxmin()
    ]

    # reassign indexes
    nearest_stations[["cell_x", "cell_y"]] = nearest_stations["from_id"].str.split(
        ",", expand=True
    )
    nearest_stations = nearest_stations.astype(dict(cell_x=int, cell_y=int))

    segments = []
    for cell_id in ddb.sql("""
        SELECT
            ns.from_id
        FROM
        nearest_stations ns
        JOIN nearest_stations nsx
        ON ns.cell_x = nsx.cell_x + 1
        AND ns.cell_y = nsx.cell_y
        AND ns.to_id <> nsx.to_id
    """).fetchall():
        coords = cells[cell_id[0]]
        segments.append((coords, Point(coords.x, coords.y + RESOLUTION)))

    for cell_id in ddb.sql("""
        SELECT
            ns.from_id
        FROM
        nearest_stations ns
        JOIN nearest_stations nsy
        ON ns.cell_y = nsy.cell_y + 1
        AND ns.cell_x = nsy.cell_x
        AND ns.to_id <> nsy.to_id
    """).fetchall():
        coords = cells[cell_id[0]]
        segments.append((coords, Point(coords.x + RESOLUTION, coords.y)))

    segments_as_features = []
    for p1, p2 in segments:
        segments_as_features.append(
            {
                "type": "Feature",
                "properties": {},
                "geometry": {
                    "coordinates": [[p1.x, p1.y], [p2.x, p2.y]],
                    "type": "LineString",
                },
            }
        )

    with open(f"areas_borders.json", "w") as fw:
        fw.write("""{
            "type": "FeatureCollection",
            "features": [""")
        fw.write(",\n".join([json.dumps(f) for f in segments_as_features]))
        fw.write("]}")
