import logging

import matplotlib as mpl
from PIL import Image, ImageDraw
from shapely import Point

import milano

# Resolution of the resulting image
RESOLUTION_X = 5000
RESOLUTION_Y = int(
    RESOLUTION_X * (milano.MAXY - milano.MINY) / (milano.MAXX - milano.MINX)
)

# maximum minuts to assign a color, more than this is grey
MAX_SCALE_TIME = 40.0
CELL_PIXEL_SIZE_X = RESOLUTION_X / ((milano.MAXX - milano.MINX) / milano.RESOLUTION)
CELL_PIXEL_SIZE_Y = RESOLUTION_Y / ((milano.MAXY - milano.MINY) / milano.RESOLUTION)

logger = logging.getLogger(__name__)


def coord_to_pixel(p: Point) -> tuple[float, float]:
    """Convert Point coordinate to a pixel coordinate for this area.

    Coordinates are the usual EPSG 4326

    NOTE: this assumes the extent is very small, enough to not introduce errors
    due to curvature. For this project is usually no bigger than a city.
    """
    x = (p.x - milano.MINX) * RESOLUTION_X / (milano.MAXX - milano.MINX)
    # computer graphics convention for Y, swap it
    y = (milano.MAXY - p.y) * RESOLUTION_Y / (milano.MAXY - milano.MINY)

    return x, y


if __name__ == "__main__":
    color_map = mpl.colormaps["viridis"]

    stations_with_coords = milano.retrieve_all_calculated_distances()
    im = Image.new("RGB", (RESOLUTION_X, RESOLUTION_Y), (0, 0, 0))

    draw = ImageDraw.Draw(im)
    for _idx, row in stations_with_coords.iterrows():
        cell_point = coord_to_pixel(row["cell_coord"])
        color = (0, 0, 0)
        travel_time = row["travel_time"]
        if travel_time < 1.0:
            color = (0, 0, 0)
        elif 1.0 <= travel_time <= MAX_SCALE_TIME:
            csvalue = color_map(travel_time / MAX_SCALE_TIME)
            color = (
                int(csvalue[0] * 255),
                int(csvalue[1] * 255),
                int(csvalue[2] * 255),
            )
        elif MAX_SCALE_TIME < travel_time:
            color = (100, 100, 100)
        if _idx % 1000 == 0:
            logger.info(f"Processing row #{_idx}")
        draw.rectangle(
            [
                cell_point,
                (cell_point[0] + CELL_PIXEL_SIZE_X, cell_point[1] + CELL_PIXEL_SIZE_Y),
            ],
            fill=color,
        )

    im.save("heatmap.png")
