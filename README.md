# Nearest Metro stop in Milan

This is a script that calculates, for each metro stop in Milan, the area for which that stop is the
quickest to reach.
"Quickest" means accounting the time to walk there or take a tram or a bus, based on public transport
schedules and road network.

![Heatmap generated with this script](/heatmap.png)

Although this script has been created to process the data for Milan specifically, it should be
trivial to adapt to other cities, you will need a GTFS file (or multiple ons) for the schedules and
a PBF extract from OpenStreetMap for the road network.

## Usage

Decompress your GTFS file into `gtfs_data`, place it compressed into `input_data` along with the pbf
file.

Install `uv` if needed, then `uv sync` to install all the dependencies. You will also need a JDK to
run r5py since it uses Java.

Run `uv run milano.py` to calculate the nearby stations for a grid of points.
The result will be a set of pickle files stored into `distances_cache`. This step can take long if
the resolution is high. In my case it took 6 hours (can be parallelized and it's trivial to skip
data already calculated, but that's not implemented).

Then `uv run generate_voronoi.py` will aggregate all the data from those pickle files and calculate
the regions for each Metro stop, placing them in a single geoJSON file and in separate files.

The line color and station names are also present in the aggregated file, to ease the representation.

Additionally, you can run `uv run generate_raster.py` to generate the heatmap.

## How does it work

[Here's a detailed article about this project](https://jacopofarina.eu/posts/calculating-reachability-metro-milan/)

It uses [r5py](https://r5py.readthedocs.io), a wrapper over the routing engine developed by
[Conveyal](https://conveyal.com/learn), to calculate the travel times between points.

A grid is generated over the city and the walking distance between those points and the Metro stops
is calculated by the engine. The process happens in chunks to make it easier to follow it and stop
and resume if needed.

Then, a set of DuckDB queries and Shapely operations calculate the areas of nearby points that all
go to the same station with the minimum time and generate a single geoJSON for it, with properties
such as the name and the color to make it trivial to display in a frontend map.
