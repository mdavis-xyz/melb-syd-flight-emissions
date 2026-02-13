# Melbourne - Sydney Flight Emissions

Many Australians say that Australia is too big for high speed rail to be worth it.
However the Melbourne-Sydney flight path is one of the busiest in the world.
The purpose of this repo is to create an animation showing how many planes are flying between the two on a normal day,
and the corresponding emissions.

I'm focused on CO2, but added the other emissions anyway:

- CO₂ (carbon dioxide): primary greenhouse gas
- NOₓ (nitrogen oxides): local air pollutant
- SOₓ (sulfur oxides): local air pollutant
- CO (carbon monoxide): greenhouse gas
- HC (hydrocarbons): greenhouse gas (unburned fuel)
- PM (particulate matter total) - local pollutant

## Input Data

We need to know:

- the positions of all commercial flights on that route
- the type of aircraft
- the emissions intensity of each type of aircraft

If you want to know the per-passenger emissions of a given flight, [Google Flights](https://www.google.com/travel/flights/) is best. 
For our purposes we want per-plane emissions. (Finding the number of passengers per plane is surprisingly difficult.)
Google's emissions model [is public](https://github.com/google/travel-impact-model/tree/main).
So we can see what sources they use.

OpenSky gives us flight data. (Their API and SDK are difficult to use, so I made a fork.)

Emissions for a given plane model and flight duration is [here](https://web.archive.org/web/20250209143401/https://www.eea.europa.eu/publications/emep-eea-guidebook-2023/part-b-sectoral-guidance-chapters/1-energy/1-a-combustion/1-a-3-a-aviation.3/view) (Wayback archive).
The way emissions data works is that there is one number for take off, taxiing, landing etc. There's another set of numbers for cruising, for a given duration/distance. We need to interpolate to figure out the cruising number, then add it to the start/end number.

## Result Data

`01-get-data.ipynb` is a jupyter notebook which does the calculations and saves two result files into `data/results`.

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
* `planes.parquet`
  * description: one row per (flight, time). The emissions values are cumulative, within each flight. So for total emissions, group by time, sum across flights.
  * columns:
    * `TIME` datetime in Sydney time zon
    * `LATITUDE`: float
    * `LONGITUDE`: float
    * `ANGLE`: Which way is the plane facing. 0 is east, 90 is north.
    * `IN_AIR`: bool

## Graph Implementation

`02-create-animation.py` takes the data and makes a video. The animation is created using:

- **matplotlib**: Main plotting library for creating the figure with map and statistics panels
- **contextily**: Adds OpenStreetMap basemap tiles to show geographic features (terrain, cities, coastline)
- **pyproj**: Coordinate transformation from lat/lon (EPSG:4326) to Web Mercator (EPSG:3857) for map projection
- **Pillow (PIL)**: Loads and rotates plane images while preserving transparency
- **ffmpeg**: Stitches PNG frames into MP4 video. (Note that `ffmpeg` needs to be installed globally.)

I wrote the data part (`01-get-data.ipynb`) myself. The animation was written by an LLM.

## Marginal Damage Costs

What price do we put on carbon and other gases?
This is saved in `02-create-animation.py`.

### Carbon Dioxide - CO2

Ideally this should be based on marginal damage.
But that's hard to estimate. I personally lean towards the higher end of estimates (1000 USD/tonne CO2e).

I don't want to use things like ACCU or ETS market prices, because they tend to be far lower than marginal damage.

However for now, I'll use a more institutional value, to match infrastructure policy estimates in Australia.

https://www.aemc.gov.au/sites/default/files/2024-03/AEMC%20guide%20on%20how%20energy%20objectives%20shape%20our%20decisions%20clean%20200324.pdf
Table A.1, page 19

80 AUD/tonne in 2026

### Carbon Monoxide - CO

CO is an indirect greenhouse gas.
See section 4.2.3 of [this IPCC report](https://www.ipcc.ch/site/assets/uploads/2018/03/TAR-04.pdf).

The IPCC estimate that it is 1.9x worse than CO2.
https://archive.ipcc.ch/publications_and_data/ar4/wg1/en/ch2s2-10-3-2.html

We use this multiplier to convert CO to CO2e, then apply the CO2 damage cost.

### SOX, NOX, PM2.5

Victorian EPA:
https://www.nepc.gov.au/sites/default/files/2022-09/aaq-nepm-impact-statement-appendix-c.pdf
They don't say the numbers, but say they use the UK's numbers:
https://www.gov.uk/government/publications/assess-the-impact-of-air-quality/air-quality-appraisal-damage-cost-guidance

In 2025 prices, pounds.


### Hydrocarbons - HC

HC (unburned fuel) is effectively NMVOC (non-methane volatile organic compounds).

Local harms: VOCs include carcinogenic PAHs, cause eye/respiratory irritation, and are ozone precursors
(VOCs + NOX + sunlight → ground-level ozone). The UK DEFRA damage cost for VOCs is £150/tonne (2025 prices).
https://www.gov.uk/government/publications/assess-the-impact-of-air-quality/air-quality-appraisal-damage-cost-guidance

Climate (indirect GHG): Like CO, NMVOCs deplete OH (extending methane lifetime) and enhance ozone.
Fry et al. (2014) estimate GWP100 of 10.5 for the Australian region. (Damages are global, but the extent of damage depends on location-specific wind flows.)
https://acp.copernicus.org/articles/14/523/2014/acp-14-523-2014.pdf table 4 AU GWP20

We use 10.5 as the GWP100, plus the local VOC damage cost.


### H20 Damage

Water vapour, spread at plane height, is a substantial greenhouse gas. However it dissappates very quickly.
Google's Travel Impact models don't include it, So I won't either.
https://travelimpactmodel.org/
More info is on page 263 of this IPCC document:
https://www.ipcc.ch/site/assets/uploads/2018/03/TAR-04.pdf

## Passenger Count

I can't find data about the number of seats on each plane. Even if I could map each flight to a plane model, each carrier uses a different seating layout.
So I'm just taking the yearly total, and dividing it to get a daily amount (treating weekdays and weekends equally), then dividing that equally amongst each plane. That's not right, but it's the best I can do.

Some flights are not passenger flights. (Note that high-speed rail could satisfy some demand for cargo flights, so including them is still valid. As far as passenger counts are concerned, the total will still be right.)

https://www.bitre.gov.au/sites/default/files/documents/domestic-aviation-activity-2024.pdf
8.04 million per year (one way).