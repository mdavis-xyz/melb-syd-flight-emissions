# Melbourne - Sydney Flight Emissions

Many Australians say that Australia is too big for high speed rail to be worth it.
However the Melbourne-Sydney flight path is one of the busiest in the world.
The purpose of this repo is to create an animation showing how many planes are flying between the two on a normal day,
and the corresponding emissions.

## Animation

**Left pane (half of width):**
- OpenStreetMap basemap showing the Melbourne-Sydney corridor with geographic features
- Melbourne and Sydney airports marked with colored dots
- Plane icons positioned at their actual lat/lon coordinates
- Planes are rotated to match their heading angle

**Right pane (half of width):**
- Current time displayed in HH:MM:SS format
- Cumulative emissions counters (odometer style with zero padding):
  - CO₂ (carbon dioxide) - primary greenhouse gas
  - NOₓ (nitrogen oxides) - air pollutant
  - SOₓ (sulfur oxides) - air pollutant
  - CO (carbon monoxide) - toxic gas
  - HC (hydrocarbons) - unburned fuel
  - PM (particulate matter total) - respiratory hazard

## Input Data

We need to know:

- the positions of all commercial flights on that route
- the type of aircraft
- the emissions intensity of each type of aircraft

This is based on a university assignment I did which was similar. See `./CBA`.

If you want to know the per-passenger emissions of a given flight, [Google Flights](https://www.google.com/travel/flights/) is best. 
For our purposes we want per-plane emissions. (Finding the number of passengers per plane is surprisingly difficult.)
Google's emissions model [is public](https://github.com/google/travel-impact-model/tree/main).
So we can see what sources they use.

OpenSky gives us flight data.

Emissions for a given plane model and flight duration is [here](https://web.archive.org/web/20250209143401/https://www.eea.europa.eu/publications/emep-eea-guidebook-2023/part-b-sectoral-guidance-chapters/1-energy/1-a-combustion/1-a-3-a-aviation.3/view) (Wayback archive).
The way emissions data works is that there is one number for take off, taxiing, landing etc. There's another set of numbers for cruising, for a given duration/distance. We need to interpolate to figure out the cruising number, then add it to the start/end number.

## Result Data

`01-get-data.ipynb` saves two result files into `data/results`.

* `emissions.parquet`
  * cumulative emissions at each moment in time (not per flight, all flights together)
  * columns:
    * `TIME`: datetime (Sydney time zone)
    * `CO2` - Carbon dioxide (kg) - primary greenhouse gas
    * `NOX` - Nitrogen oxides (kg) - air pollutant, contributes to smog and acid rain
    * `SOX` - Sulfur oxides (kg) - air pollutant, contributes to acid rain
    * `H2O` - Water vapor (kg)
    * `CO` - Carbon monoxide (kg) - toxic gas, air pollutant
    * `HC` - Hydrocarbons (kg) - unburned fuel, air pollutant
    * `PM_NON_VOLATILE` - Particulate matter non-volatile (kg)
    * `PM_VOLATILE` - Particulate matter volatile (kg)
    * `PM_TOTAL` - Particulate matter total (kg) - air pollutant, respiratory hazard
datetime[μs, Australia/Sydney]	f64	f64	f64	f64	f64	f64	f64	f64	f64
* `planes.parquet`
  * description: one row per (flight, time). The emissions values are cumulative, within each flight. So for total emissions, group by time, sum across flights.
  * columns:
    * `TIME` datetime in Sydney time zon
    * `LATITUDE`: float
    * `LONGITUDE`: float
    * `ANGLE`: Which way is the plane facing. 0 is east, 90 is north.
    * `IN_AIR`: bool

## Graph Implementation

The animation is created using:

- **matplotlib**: Main plotting library for creating the figure with map and statistics panels
- **contextily**: Adds OpenStreetMap basemap tiles to show geographic features (terrain, cities, coastline)
- **pyproj**: Coordinate transformation from lat/lon (EPSG:4326) to Web Mercator (EPSG:3857) for map projection
- **Pillow (PIL)**: Loads and rotates plane images while preserving transparency
- **ffmpeg**: Stitches PNG frames into MP4 video

### Approach

1. Generate one PNG frame per timestamp using matplotlib
2. Left panel: matplotlib plot with contextily basemap showing the Sydney-Melbourne corridor
3. Right panel: matplotlib text annotations showing time and emissions counters
4. Plane images are rotated based on heading and overlaid on the map
5. All frames are combined into video using ffmpeg command-line tool

This approach was chosen because:
- matplotlib handles both map plotting and text rendering
- contextily provides easy integration with tile-based maps (like Google Maps style)
- No heavy GIS libraries needed for this simple corridor visualization
- ffmpeg is industry-standard and already installed on the system