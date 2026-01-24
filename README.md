# Melbourne - Sydney Flight Emissions

Many Australians say that Australia is too big for high speed rail to be worth it.
However the Melbourne-Sydney flight path is one of the busiest in the world.
The purpose of this repo is to create an animation showing how many planes are flying between the two on a normal day,
and the corresponding emissions.

## Animation

Left pane (2/3 of width):
A map of eastern Australia, with Melbourne and Sydney marked.
Little pictures of planes are shown zooming across the map, corresponding to real flights.
The flight number is annotated as a textbox next to it.

Right pane:
An analog clock showing the time (with AM/PM embedded in its centre)
A counter showing number of planes so far, and cumulative emissions.

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
    * `CO2` - CO2 kg
    * `NOX` - nitrous oxides
    * `SOX` - sulfur oxides
    * `H2O`
    * `CO` - carbon monoxide
    * `HC`
    * `PM_NON_VOLATILE` - particular matter
    * `PM_VOLATILE` - particular matter
    * `PM_TOTAL`- particular matter
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

Which library(s)?