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


I downloaded data about flights (departure and arrival time, departure and arrival airport) from [OpenSky Network](https://opensky-network.org/). Their underlying dataset is free, useful and impresive. However their API and [Python client library](https://openskynetwork.github.io/opensky-api/index.html) is quite buggy and difficult to use, so I made [my own fork](https://github.com/mdavis-xyz/opensky-api/tree/fork).
Their free tier does not expose the detailed flight path. I only have the start and end points, and assume a linear path in between for the animation.

For each model of aircraft, I obtained the emissions intensity from [the European Environment Agency](https://www.eea.europa.eu/publications/emep-eea-guidebook-2023/part-b-sectoral-guidance-chapters/1-energy/1-a-combustion/1-a-3-a-aviation.3/view) via [The Wayback Machine](https://web.archive.org/web/20260000000000*/https://www.eea.europa.eu/publications/emep-eea-guidebook-2023/part-b-sectoral-guidance-chapters/1-energy/1-a-combustion/1-a-3-a-aviation.3/at_download/file). This splits emissions into the "taxiing, take off and landing" phase (a fixed amount at each airport) and the "cruise, control and descent" phase in the middle, for different lengths of journey.

The way emissions data works is that there is one number for take off, taxiing, landing etc. There's another set of numbers for cruising, for a given duration/distance. We need to interpolate to figure out the cruising number, then add it to the start/end number.

Airport IDs and locations are in `airports.json` (from Wikipedia).

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

## List of Airports

If you want to do the same analysis for airports in another region:

1. Change `airport_info` in `01-get-data.ipynb`, and perhaps other fields such as timezone, to get the data. This is also where the date is chosen.
2. Change `MELBOURNE_Y`, `SYDNEY_X` etc in `02-create-animation.py` to pan the map.


## Marginal Damage Costs

What price do we put on carbon and other gases?
This is saved in `02-create-animation.py`.

### Carbon Dioxide - CO2

Ideally this should be based on marginal damage.
But that's hard to estimate. Estimates vary a lot, between 80 USD/tonne to 1000 USD/tonne and even higher.
I personally lean towards the higher end of estimates. Here I choose 308 USD/tonne from [Rennert et al. 2022](https://www.nature.com/articles/s41586-022-05224-9).

* Most academic economists are to used to thinking on the margin. They're used to thinking about how a 1% change in x causes a 0.1% change in y. However, climate change is not marginal. When correlated increases in extreme weather cause the reinsurance markets to collapse, that's not marginal. When glaciers in India and Pakistan start melting earlier, becoming misaligned with crop cycles, the hundreds of millions of refugees that will flee will not be marginal. When rising ocean levels cause a trillion dollars of property damage, that's not marginal.
* Academic economists tend to think in expected value. For most things that's fine, but when the long tail is catastrophic, that is not. If you model based on the 50th percentile, that will not capture how catastrophically bad the 98th percentil is. Insurers and risk-focused companies such as electricity retailers know this. They don't care about the POE50, they care about POE90, POE95, POE99.9. Once we get into those ballparks, the extinction of humanity is what we're talking about.
* Discount rates and other parameters tend to be based on the assumption that people in the future will be wealthier. Catastrophic climate change means that this is not a valid assumption. Given that these estimates impact whether we have runaway climate change and breach tipping points of not, there's a circular nature to this logic. We assume things in the future will be hunkey-dorey, and then ta-da, the conclusion is that things in the future will be not so bad. Another typical assumption is that population will grow faster than than the discount rate, which tends to be motivated solely by the fact that if you don't assume this the maths breaks. This is a questionable assumption
* Damage estimates are often based on GDP, and thus exclude non-monetary damage. e.g. they assume that the death of a person is only as bad as their foregone consumption.

Institutional prices (e.g. EU-ETS and ACCUs) are far lower than marginal damage and the expected price of carbon, because they are set by unambitious politicians. Just think about the incentives of a politician with a 3-6 year election cycle. Will they overestimate or underestimate the importance of long term climate damage? Yes, academics estimating marginal damage have poor incentives too (you won't get tenure if your calculations yield an estimate with a different number of zeros to your peers), but still, I trust the academics more. Anchoring/status-quo bias is ex-ante not of a given direction.

But since those numbers drive government investment decisions, I also did analysis with that.
The AEMC uses a price of [80 AUD/tonne in 2026](https://www.aemc.gov.au/sites/default/files/2024-03/AEMC%20guide%20on%20how%20energy%20objectives%20shape%20our%20decisions%20clean%20200324.pdf) (Table A.1, page 19).
Note that the shadow price of carbon should be indexed aggressively over time, far beyond inflation, because marginal damage increases with CO2 stock. Since high speed rail would abate carbon in the future, that means the cost of emissions of potentially-abated planes should be indexed far higher, for a future price. However, I did not do this. I used today's price, to be conservative.

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

For NOX and PM2.5, we use aircraft specific figures.

In 2025 prices, pounds.

### Why is NOX damage so big?

I was surprised that it's so big. 3x bigger than C02 damage when using the AER carbon price.
Here's a paper showing that this is indeed right:
https://www.sciencedaily.com/releases/2019/11/191107202553.htm

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
So I'm just taking the yearly total, and dividing it to get a daily amount (treating weekdays and weekends equally), then dividing that equally amongst each plane between Sydney and Melbourne. Then I assume that each plane to and from Canberra has the same average number of passengers. That's not very sophisticated, but it's the best I can do. The purpose of this project is to focus on emissions, not passenger count.

Some flights are not passenger flights. (Note that high-speed rail could satisfy some demand for cargo flights, so including them is still valid. As far as passenger counts are concerned, the total will still be right.)

https://www.bitre.gov.au/sites/default/files/documents/domestic-aviation-activity-2024.pdf
8.04 million per year (one way).

## Changing Airports

If you want to reproduce this analysis and animation for some other airports:

-  modify `airports.json`
- modify the passenger count estimates in `01-get-data.ipynb`, which are specific to Sydney-Melbourne
- Create your own (free) OpenSky account and credentials [here](https://openskynetwork.github.io/opensky-api/rest.html#oauth2-client-credentials-flow)
- Modify the latitude and longitude in `02-create-animation.py`.