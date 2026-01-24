#!/usr/bin/env python
# coding: utf-8

# # Flight Emissions Data
# 
# We get flight data from OpenSky. We merge that with Europa data about emissions intensity by aircraft model.
# 

# ## Imports and Constants

# In[1]:


import datetime as dt
import zoneinfo
import json
# import json
# from pprint import pprint
# import urllib.request 
import os
# from statistics import median, mean
import logging

from tqdm import tqdm
import polars as pl
from opensky_api import OpenSkyApi


# In[2]:


# will be created if doesn't exist
data_dir = "data"

# from here: https://opensky-network.org/datasets/#metadata/aircraftDatabase
mapping_csv = os.path.join(data_dir, "raw/aircraftDatabase.csv.gz")

# from here: https://www.eea.europa.eu/publications/emep-eea-guidebook-2023/part-b-sectoral-guidance-chapters/1-energy/1-a-combustion/1-a-3-a-aviation.3/view
# https://www.eea.europa.eu/publications/emep-eea-guidebook-2023/part-b-sectoral-guidance-chapters/1-energy/1-a-combustion/1-a-3-a-aviation.3/at_download/file
emissions_spreadsheet = os.path.join(data_dir, "raw/1.A.3.a Aviation -Annex 1 - Master emissions calculator - 2023 - Protected - v1.5_18_09_2024.xlsx")

# downloaded from
# https://data.cso.ie/
# discovered from Wikipedia: https://en.wikipedia.org/wiki/Dublin_Airport#Statistics
passenger_numbers_sheet = os.path.join(data_dir, "popular_destinations.csv")

opensky_api_secret_file = "/home/matthew/.local/share/credentials/opensky_api_key.json"

results_dir = os.path.join(data_dir, "results")
emissions_results_path = os.path.join(results_dir, "emissions.parquet")
planes_results_path = os.path.join(results_dir, "planes.parquet")


# In[3]:


# microseconds per second
us_per_s = 1e6

# Melbourne and Sydney are the same timezone
tz_name = 'Australia/Sydney' 


# In[4]:


airport_info = {
    # https://en.wikipedia.org/wiki/Melbourne_Airport
    "Melbourne": {
        "IATA": "MEL",
        "ICAO": "YMML",
        "WMO": 94866,
        "location": {
            'latitude': -37.673333, 
            'longitude': 144.843333,
        }
    },
    # https://en.wikipedia.org/wiki/Sydney_Airport
    "Sydney": {
        "IATA": "SYD",
        "ICAO": "YSSY",
        "WMO": 94767,
        "location": {
            'latitude': -33.946111, 
            'longitude': 151.177222
        }
    }
}

# arbitrarily choose Sydney as the airport
# look at all flights to/from Sydney from/to anywhere
# when later filter by from/to Melbourne
airport_id_type = "ICAO"
airport_id = airport_info['Sydney'][airport_id_type]
other_airport_id = airport_info['Melbourne'][airport_id_type]


# In[5]:


sydney_tz = zoneinfo.ZoneInfo('Australia/Sydney')


# ## Download flight data from Open Sky
# 
# The OpenSky API is documented [here](https://openskynetwork.github.io/opensky-api/python.html#opensky_api.FlightData).
# 

# In[ ]:


logger = logging.getLogger("opensky_api")
#logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)


# In[7]:


api = OpenSkyApi(client_json_path=opensky_api_secret_file)


# In[ ]:


_date = dt.date.today() - dt.timedelta(days=100)


# must include no more than 2 days in the timespan
# time_start = dt.datetime(2025, 10, 10, 0, 0, tzinfo=sydney_tz)
# time_end = dt.datetime(2025, 10, 15, 23, 59, 59, tzinfo=sydney_tz)
# time_start = dt.datetime.combine(_date, dt.time.min, tzinfo=sydney_tz)
# time_end = dt.datetime.combine(_date, dt.time.max, tzinfo=sydney_tz)

# time_start = dt.datetime.combine(_date, dt.time.min)
# time_end = dt.datetime.combine(_date, dt.time.max)

def get_epoch(t: dt.datetime) -> int:
    return round(t.timestamp())


# In[ ]:


today = dt.date.today()
_dates = [today - dt.timedelta(days=d) for d in range(95, 100)]


# In[ ]:


responses = []

for _date in tqdm(_dates):
    start_epoch = get_epoch(dt.datetime.combine(_date, dt.time.min))
    end_epoch = get_epoch(dt.datetime.combine(_date, dt.time.max))

    responses.extend(
        api.get_departures_by_airport(
            airport=airport_id, begin=start_epoch, end=end_epoch
        )
    )
    responses.extend(
        api.get_arrivals_by_airport(
            airport=airport_id, begin=start_epoch, end=end_epoch
        )
    )


# In[ ]:


flight_api_data = pl.LazyFrame([f.__dict__ for f in responses])
flight_api_data.sink_csv(os.path.join(data_dir, "api_raw.csv"))
flight_api_data.sink_parquet(os.path.join(data_dir, "api_raw.parquet"))


# ## Process Flight Data
# 
# You can skip the previous section and start from here to read cached data from disk.

# Parse datetimes, calculate duration. (e.g. `firstSeen` actually is departure time.)
# We delete in-progress flights, because that duration will be wrong.

# In[8]:


flights = (
    pl.scan_parquet(os.path.join(data_dir, "api_raw.parquet"))
    .filter(pl.col("estArrivalAirport").is_not_null())
    .filter(pl.col("estArrivalAirport") != pl.col("estDepartureAirport"))
    .with_columns(
        (pl.col("firstSeen") * us_per_s).cast(
            pl.Datetime()
        ).dt.convert_time_zone("Australia/Sydney"),
        (pl.col("lastSeen") * us_per_s).cast(
            pl.Datetime()
        ).dt.convert_time_zone("Australia/Sydney"),
    )
    .with_columns(
        (pl.col("lastSeen") - pl.col("firstSeen")).alias("flightDurationActual")
    )
    .select(
        pl.col("icao24").alias("ICAO24_HEX"),
        pl.col("firstSeen").alias("DEPARTURE_TIME"),
        pl.col("lastSeen").alias("ARRIVAL_TIME"),
        pl.col("flightDurationActual").alias("FLIGHT_DURATION_ACTUAL"),
        pl.col("estDepartureAirport").alias("DEPARTURE_AIRPORT"),
        pl.col("estArrivalAirport").alias("ARRIVAL_AIRPORT"),
    )
)
flights.head().collect()


# In[9]:


(
    flights
    .select(
        pl.col("ARRIVAL_TIME").min().alias("ARRIVAL_MIN"),
        pl.col("ARRIVAL_TIME").max().alias("ARRIVAL_MAX"),
        pl.col("DEPARTURE_TIME").min().alias("DEPARTURE_MIN"),
        pl.col("DEPARTURE_TIME").max().alias("DEPARTURE_MAX"),
    )
    .collect()
)


# In[10]:


# filter to flights between Melbourne and Sydney
# and during the time window (API might return extra)
flights = (
    flights
    .filter(
        pl.any_horizontal(
            (pl.col("DEPARTURE_AIRPORT") == pl.lit(airport_id))
            &
            (pl.col("ARRIVAL_AIRPORT") == pl.lit(other_airport_id)),
            (pl.col("DEPARTURE_AIRPORT") == pl.lit(other_airport_id))
            &
            (pl.col("ARRIVAL_AIRPORT") == pl.lit(airport_id))
        )
    )
    # .filter(pl.col("DEPARTURE_TIME") >= time_start)
    # .filter(pl.col("ARRIVAL_TIME") >= time_end)
)
flights.head().collect()


# ## Join to emissions data

# We download a mapping file from OpenSky, to map icao24 IDs to flight model.
# Browse the files [here](https://opensky-network.org/datasets/#metadata/aircraftDatabase).
# With that page, `aircraftDatabase.csv` came from [here]("https://s3.opensky-network.org/data-samples/metadata/aircraftDatabase.csv").
# 
# Within this CSV:
# 
# * `icao24` matches the OpenSki API
# * `typecode` matches what's in the emissions spreadsheet as `ICAO_24`

# In[11]:


def download_file(url, path):
    dir_path = os.path.dirname(path)
    os.makedirs(dir_path, exist_ok=True)
    if not os.path.exists(path):
        urllib.request.urlretrieve(url, path)


# In[12]:


# https://opensky-network.org/datasets/#metadata/aircraftDatabase
url = "https://s3.opensky-network.org/data-samples/metadata/aircraftDatabase.csv"
download_file(url, mapping_csv)


# In[13]:


mapping_df = (
    pl.scan_csv(mapping_csv, skip_rows_after_header=1)
    .select(
        pl.col("icao24").alias("ICAO24_HEX"),
        pl.col("typecode").alias("ICAO_OTHER")
    )
)
mapping_df.head().collect()


# In[14]:


flight_and_type = flights.join(mapping_df, on="ICAO24_HEX", how="left")
flight_and_type.head().collect()


# In[15]:


# from here: https://www.eea.europa.eu/publications/emep-eea-guidebook-2023/part-b-sectoral-guidance-chapters/1-energy/1-a-combustion/1-a-3-a-aviation.3/view
url = "https://www.eea.europa.eu/publications/emep-eea-guidebook-2023/part-b-sectoral-guidance-chapters/1-energy/1-a-combustion/1-a-3-a-aviation.3/at_download/file"
download_file(url, emissions_spreadsheet)


# In[16]:


# This sheet in the file is hidden in Excel
# Extract it manually.
(
    pl.read_excel(
        source=emissions_spreadsheet,
        sheet_name="database",
        read_options={
            "skip_rows": 1,
            "header_row": True,
        },
        schema_overrides={"FORECAST  DURATION": pl.Duration()},
    )
    .write_excel(os.path.join(data_dir, "spreadsheet-extracted.xlsx"))
)


# In[17]:


# load the emissions spreadsheet
emissions_lookup = (
    pl.read_excel(
        source=emissions_spreadsheet,
        sheet_name="database",
        read_options={
            "skip_rows": 1,
            "header_row": True,
        },
        schema_overrides={"FORECAST  DURATION": pl.Duration()},
    )
    .lazy()
    .filter(pl.col("ICAO_ID").is_not_null())
    .filter(pl.col("ICAO_ID") != "")
    .rename(
        {
            "ICAO_ID": "ICAO_OTHER",
            "AIRCRAFT ID": "AIRCRAFT_ID",
            # watch out, there are double spaces and trailing spaces
            # in the raw data
            "FORECAST  DURATION": "DURATION_REFERENCE",
            "FORECAST CO2 (3,15 for JET-A or 3,10 for AvGAS) ": "CO2",
            "FORECAST  NOX": "NOX",
            "FORECAST  SOX": "SOX",
            "FORECAST  H20": "H2O",
            "FORECAST  CO": "CO",
            "FORECAST  HC": "HC",
            " PM Non Volatile": "PM_NON_VOLATILE",
            "PM VOLATILE (all)": "PM_VOLATILE",
            "PM TOTAL": "PM_TOTAL",
        }
    )
)

emissions_cols = [
    "CO2",
    "NOX",
    "SOX",
    "H2O",
    "CO",
    "HC",
    "PM_NON_VOLATILE",
    "PM_VOLATILE",
    "PM_TOTAL",
]

emissions_lookup.head().collect()


# Now we do the joins.
# 
# Some ICAO_OTHER hex values have a mapping to CCD but not LTO. Use that to get the Aircraft ID. Then look up aircraft ID if there's no match based on ICAO_OTHER.
# 
# "LTO" is landing and take off (the emissions while at the airport). "CCD" is cruise, control and descent (mid-flight).
# I don't know what `LTO2` is, which is sometimes missing. So we just use `LTO`.

# In[18]:


# look up takeoff and landing
flight_and_type = (
    flight_and_type
    .join(
        emissions_lookup
        .select("ICAO_OTHER", "AIRCRAFT_ID")
        .unique()
        , on="ICAO_OTHER", how="left"
    )
)
flight_and_type.head().collect()


# In[19]:


# look up takeoff and landing emissions
flight_and_type = (
    flight_and_type
    .join(
        emissions_lookup
        .filter(pl.col("LTO or CCD") == "LTO")
        .select(["AIRCRAFT_ID"] + [pl.col(c).alias(c + "_LTO") for c in emissions_cols])
        , on="AIRCRAFT_ID", how="left"
    )
)
flight_and_type.head().collect()


# Cruise, control and descent (CCD) is harder. Most have many matches, for varying duration/distance. We need to match to two rows, and linearly interpolate in between them.
# 

# In[20]:


emissions_options = (
    flight_and_type
    .join(emissions_lookup,
          on="AIRCRAFT_ID",
          how="left"
    )
)


# In[21]:


emissions_lower = (
    emissions_options
    .sort("DURATION_REFERENCE", descending=False)
    .filter(pl.col("DURATION_REFERENCE") >= pl.col("FLIGHT_DURATION_ACTUAL"))
    .group_by("AIRCRAFT_ID", "FLIGHT_DURATION_ACTUAL")
    .first()
    .select(
        ["AIRCRAFT_ID", "FLIGHT_DURATION_ACTUAL", pl.col("DURATION_REFERENCE").alias("DURATION_REFERENCE_LOWER")]
        + [pl.col(c).alias(c + "_LOWER") for c in emissions_cols]
    )
)

emissions_upper = (
    emissions_options
    .sort("DURATION_REFERENCE", descending=True)
    .filter(pl.col("DURATION_REFERENCE") <= pl.col("FLIGHT_DURATION_ACTUAL"))
    .group_by("AIRCRAFT_ID", "FLIGHT_DURATION_ACTUAL")
    .first()
    .select(
        ["AIRCRAFT_ID", "FLIGHT_DURATION_ACTUAL", pl.col("DURATION_REFERENCE").alias("DURATION_REFERENCE_UPPER")]
        + [pl.col(c).alias(c + "_UPPER") for c in emissions_cols]
    )
)


# In[22]:


flight_emissions = (
    flight_and_type
    .join(emissions_lower, on=["AIRCRAFT_ID", "FLIGHT_DURATION_ACTUAL"], how="left")
    .join(emissions_upper, on=["AIRCRAFT_ID", "FLIGHT_DURATION_ACTUAL"], how="left")
    .with_columns(
        ((pl.col("FLIGHT_DURATION_ACTUAL") - pl.col("DURATION_REFERENCE_LOWER")) / (pl.col("DURATION_REFERENCE_UPPER") - pl.col("DURATION_REFERENCE_LOWER"))).alias("INTERPOLATION_FACTOR")
    )
    .with_columns(
        [
            (pl.col(c + "_LOWER") + pl.col("INTERPOLATION_FACTOR") * (pl.col(c + "_UPPER") - pl.col(c + "_LOWER"))).alias(c + "_CCD")
            for c in emissions_cols
        ]
    )
    .select(
        pl.exclude([c + app for c in emissions_cols for app in ["_UPPER", "_LOWER"]])
    )
)
flight_emissions.head().collect()


# In[ ]:





# In[23]:


# Fill in empty values with the mean of the column
cols_to_fill = [c + app for c in emissions_cols for app in ["_LTO", "_CCD"]]
flight_emissions = (
    flight_emissions
    .with_columns(
        pl.col(cols_to_fill).fill_null(pl.col(cols_to_fill).mean())
    )
)


# ## Sanity Check
# 
# What's the total emissions per day?
# 
# The CO2 in the raw data is in kg.

# In[24]:


(
    flight_emissions
    .group_by(pl.col("DEPARTURE_TIME").dt.date().alias("DEPARTURE_DATE"))
    .agg([
        (pl.col(c + "_LTO").sum() + pl.col(c + "_CCD").sum()).alias(c)
        for c in emissions_cols
    ])
    .sort("DEPARTURE_DATE")
    .collect()
)


# So there's around 2.5 thousand tonnes of CO2 burnt per day.
# 
# Is that the right ballpark?
# 
# [Google Flights](https://www.google.com/travel/flights/search?tfs=CBwQAholEgoyMDI1LTExLTEyKABqDAgDEggvbS8wNnk1N3IHCAESA01FTEABSAFwAYIBCwj___________8BmAEC) says per-passenger emissions are around 77kg on average.
# 
# How many flights? Google Flights says 34 + 24 + 13 = 71 from Syd to Melb on 12/11/2025. Double that, it's 142 each way. 
# 
# How many seats per flight? Google Flights assumes economy seats. How do they allocate emissions between classes? Maybe 160 passengers ([Wikipedia](https://en.wikipedia.org/wiki/Airbus_A320_family)).

# In[25]:


flights_per_day = 71 * 2
passengers_per_flight = 160
emissions_per_passenger = 77

emissions_per_day = emissions_per_passenger * passengers_per_flight * flights_per_day
emissions_per_day


# Ok, so we're in the right ballpark.

# ## Airport Data
# 
# Add data about the airports to each flight

# In[26]:


airport_locations = pl.DataFrame([
    {
        "AIRPORT_ID": v[airport_id_type],
        "LATITUDE": v['location']['latitude'],
        "LONGITUDE": v['location']['longitude'],
    }
    for v in airport_info.values()
])
airport_locations


# In[27]:


flight_emissions = (
    flight_emissions
    .join(
        airport_locations
            .lazy()
            .rename({"AIRPORT_ID": "DEPARTURE_AIRPORT", 
                                   "LATITUDE": "LATITUDE_DEPARTURE",
                                   "LONGITUDE": "LONGITUDE_DEPARTURE"}),
        on="DEPARTURE_AIRPORT"
    )
    .join(
        airport_locations
            .lazy()
            .rename({"AIRPORT_ID": "ARRIVAL_AIRPORT",
                                   "LATITUDE": "LATITUDE_ARRIVAL", 
                                   "LONGITUDE": "LONGITUDE_ARRIVAL"}),
        on="ARRIVAL_AIRPORT"
    )
    # calculate the angle
    # 0 is horizontal
    # +1 is one degree above horizontal 
    .with_columns(
        (pl.col("LATITUDE_ARRIVAL") - pl.col("LATITUDE_DEPARTURE")).alias("LATITUDE_DELTA"),
        (pl.col("LONGITUDE_ARRIVAL") - pl.col("LONGITUDE_DEPARTURE")).alias("LONGITUDE_DELTA"),
    )
    .with_columns(
        (pl.col("LATITUDE_DELTA") / pl.col("LONGITUDE_DELTA")).arctan().degrees().alias("ANGLE")
    )
)


# ## Mid-Flight Data
# 
# We generate a list of timestamps which we care about.
# 
# OpenSky doesn't offer mid-flight positions for historcal flights, so we'll just interpolate.

# In[28]:


# choose the second date
# (first might be partial day because of timezone boundaries)
_date = (
    flight_emissions
    .select(pl.col("DEPARTURE_TIME").dt.date().alias("DATE"))
    .sort("DATE")
    .head(2)
    .tail(1)
    .collect()
    .item()
)


# In[29]:


num_slices = 4 # 24 * 60

times_lf = pl.LazyFrame({
    "TIME": pl.datetime_range(
        start=dt.datetime.combine(_date, dt.time.min, tzinfo=sydney_tz),
        end=dt.datetime.combine(_date, dt.time.max, tzinfo=sydney_tz),
        interval=dt.timedelta(days=1) / (num_slices - 1),
        eager=True
    )
})

times_lf.collect()


# In[30]:


animation_data = (
    times_lf
    .join(flight_emissions, how="cross")
    .with_columns(
        (pl.col("TIME") >= pl.col("DEPARTURE_TIME")).alias("HAS_DEPARTED"),
        (pl.col("TIME") >= pl.col("ARRIVAL_TIME")).alias("HAS_ARRIVED"),
        pl.col("TIME").is_between("DEPARTURE_TIME", "ARRIVAL_TIME").alias("IN_AIR"),
        ((pl.col("TIME") - pl.col("DEPARTURE_TIME")) / (pl.col("ARRIVAL_TIME") - pl.col("DEPARTURE_TIME")))
        .clip(lower_bound=0, upper_bound=2)
        .alias("FLIGHT_PROGRESS")
    )
    .with_columns([
        (
            pl.col("FLIGHT_PROGRESS") * pl.col(c + "_CCD")
            + pl.col("HAS_DEPARTED") * pl.col(c + "_LTO")
        ).alias(c)
        for c in emissions_cols
    ])
    .with_columns(
        (pl.col("LONGITUDE_DEPARTURE") + pl.col("FLIGHT_PROGRESS") * (pl.col("LONGITUDE_ARRIVAL") - pl.col("LONGITUDE_DEPARTURE"))).alias("LONGITUDE"),
        (pl.col("LATITUDE_DEPARTURE") + pl.col("FLIGHT_PROGRESS") * (pl.col("LATITUDE_ARRIVAL") - pl.col("LATITUDE_DEPARTURE"))).alias("LATITUDE"),
    )
    .sort("TIME", "DEPARTURE_TIME", "ICAO24_HEX")
)
animation_data.collect()


# `animation_data` has one row per (flight, time). The emissions values are cumulative, within each flight. So for total emissions, group by time, sum across flights.

# In[31]:


os.makedirs(results_dir, exist_ok=True)


# In[32]:


(
    animation_data
    .group_by("TIME")
    .agg([
        pl.col(c).sum()
        for c in emissions_cols
    ])
    .sort("TIME")
    .sink_parquet(emissions_results_path)
)
animation_emissions_data = pl.read_parquet(emissions_results_path)
animation_emissions_data


# In[33]:


(
    animation_data
    .sort("ICAO24_HEX", "DEPARTURE_TIME", "TIME")
    .select("TIME", "LATITUDE", "LONGITUDE", "ANGLE", "IN_AIR")
    .sink_parquet(planes_results_path)
)
animation_plane_data = pl.read_parquet(planes_results_path)
animation_plane_data

