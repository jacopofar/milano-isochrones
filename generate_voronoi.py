import json
import logging

from scipy.spatial import Voronoi
from shapely import Polygon, to_geojson, union_all
import numpy as np

import milano

logger = logging.getLogger(__name__)

# stations covered by multiple lines appear with separate stops
# aggregate them now (ideally was to be done earlier)s
COLLAPSE_STATIONS = {
    "LORETO M1 (M1)": "LORETO M2 (M2)",
    "CADORNA FN M1 (M1)": "CADORNA FN M2 (M2)",
    "DUOMO M1 (M1)": "DUOMO M3 (M3)",
}

# handmade polygon around Milan to cut of many geometries and reduce complexity
# on the final geoJSON. Made by drawing it on geojson.io and copying the data
# it covers the Milan city proper (comune) plus some margin
URBAN_AREA_POLYGON = Polygon(
    [
        [
            9.100515801921745,
            45.5455167557441
        ],
        [
            9.062207888287645,
            45.516984060447925
        ],
        [
            9.087815685590783,
            45.47560988804153
        ],
        [
            9.087812727224701,
            45.46081938203761
        ],
        [
            9.084282728146945,
            45.43639822910043
        ],
        [
            9.09621866507851,
            45.412940445063015
        ],
        [
            9.137006591661759,
            45.39614314339252
        ],
        [
            9.175691848973429,
            45.39811517264238
        ],
        [
            9.238646294931186,
            45.407744335864436
        ],
        [
            9.298791777942,
            45.42551720455029
        ],
        [
            9.288229251555435,
            45.43761547988808
        ],
        [
            9.306139048795416,
            45.47585321937714
        ],
        [
            9.299049031254754,
            45.51946151209026
        ],
        [
            9.279346516602999,
            45.53914891890602
        ],
        [
            9.227661809292755,
            45.55757180067263
        ],
        [
            9.100515801921745,
            45.5455167557441
        ]
    ]
)
if __name__ == "__main__":
    bbox = Polygon(
        [
            [milano.MINX, milano.MINY],
            [milano.MAXX, milano.MINY],
            [milano.MAXX, milano.MAXY],
            [milano.MINX, milano.MAXY],
            [milano.MINX, milano.MINY],
        ]
    )
    stations_with_coords = milano.retrieve_all_calculated_distances()
    points = [(sc.x, sc.y) for sc in stations_with_coords["cell_coord"]]
    logger.info(
        f"Aggregated everything in {len(points)} total. Calculating Voronoi subdivision..."
    )
    vor = Voronoi(points)
    # now vor.vertices are all the vertices of voronoi triangles
    # in this case 5068
    # vor.ridge_vertices are the indexes of vertices, to be taken from vor.vertices
    # in this case 10067
    # they can be -1 for "degenerate" edges
    # vor.ridge_points indicates for each element in ridge_vertices which original
    # points are the on the two sides. This is also 10067
    # vor.regions contains the indexes of the vertices for each region, -1 for infinite ones
    # let's draw the triangles without caring about the regions
    # vor.point_region associates each input point to the region around it
    regions_polygons: dict[str, list[Polygon]] = dict()
    for point_idx, r in enumerate(vor.point_region):
        r = vor.regions[r]
        # ignore infinite/open regions
        if -1 in r or len(r) == 0:
            continue
        station_name = stations_with_coords.iloc[point_idx]["to_id"]
        if station_name in COLLAPSE_STATIONS:
            station_name = COLLAPSE_STATIONS[station_name]

        # add again the first element to close the polygon
        new_polygon = Polygon(np.concat([vor.vertices[r], [vor.vertices[r[0]]]]))
        # numeric error introduce weird polygons outside the region
        if not bbox.contains(new_polygon):
            continue
        if station_name not in regions_polygons:
            regions_polygons[station_name] = []
        if URBAN_AREA_POLYGON.contains(new_polygon):
            regions_polygons[station_name].append(new_polygon)
    for station_name in regions_polygons:
        with open(f"stations_polygons/{station_name}.json", "w") as fw:
            fw.write(to_geojson(union_all(regions_polygons[station_name]), indent=2))
    all_stations_obj = {"type": "FeatureCollection", "features": []}
    logger.info(
        f"All data aggregated into {len(regions_polygons)} regions. Writing the JSON files..."
    )
    regions_colors: dict[str, str] = dict()
    for station_name in regions_polygons:
        station_color = ""
        if station_name.endswith("(M1)"):
            station_color = "red"
        elif station_name.endswith("(M2)"):
            station_color = "green"
        elif station_name.endswith("(M3)"):
            station_color = "yellow"
        elif station_name.endswith("(M4)"):
            station_color = "blue"
        elif station_name.endswith("(M5)"):
            station_color = "lilac"
        else:
            raise ValueError(f"Do not know the color for {station_name}")
        regions_colors[station_name] = station_color
    for idx, station_name in enumerate(regions_polygons):
        # tried, the file gets 3-4 times smaller but looks bad
        # simplified = simplify(union_all(regions_polygons[station_name]), milano.RESOLUTION/2)
        simplified = union_all(regions_polygons[station_name])
        this_station_obj = json.loads(
            to_geojson(simplified)
        )
        assert isinstance(all_stations_obj["features"], list)
        all_stations_obj["features"].append(
            {
                "type": "Feature",
                "properties": {
                    "name": station_name,
                    "color": regions_colors[station_name],
                },
                "id": idx,
                "geometry": this_station_obj,
            }
        )
    with open("stations_polygons/all_combined.json", "w") as fw:
        fw.write(json.dumps(all_stations_obj, indent=2))
