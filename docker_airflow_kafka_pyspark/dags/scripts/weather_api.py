# https://open-meteo.com/en/docs
# !pip install openmeteo-requests
# !pip install requests-cache retry-requests numpy pandas

import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
from datetime import datetime, timedelta

def weather_api_extract():
    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
    retry_session = retry(cache_session, retries = 3, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)

    url = "https://api.open-meteo.com/v1/forecast"

    # find yesterday hourly weather
    params = {
        "latitude": 39.9523,
        "longitude": -75.1638,
        "hourly": ["temperature_2m", "relative_humidity_2m", "rain", "snowfall", "cloud_cover"],
        "timezone": "America/New_York",
        "past_days": 1,
        "forecast_days": 0
    }
    responses = openmeteo.weather_api(url, params=params)
    response = responses[0]

    # process hourly data, the params here must match what was requested
    hourly = response.Hourly()
    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy().astype(float)
    hourly_relative_humidity_2m = hourly.Variables(1).ValuesAsNumpy().astype(int)
    hourly_rain = hourly.Variables(2).ValuesAsNumpy().astype(float)
    hourly_snowfall = hourly.Variables(3).ValuesAsNumpy().astype(float)
    hourly_cloud_cover = hourly.Variables(4).ValuesAsNumpy().astype(int)
    
    date_range=pd.date_range(
	start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
	end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
	freq = pd.Timedelta(seconds = hourly.Interval()),
	inclusive = "left"
    )
    hourly_data = {"date": [x.isoformat() for x in date_range]}
    hourly_data["temperature_2m"] = list(hourly_temperature_2m)
    hourly_data["relative_humidity_2m"] = list(hourly_relative_humidity_2m)
    hourly_data["rain"] = list(hourly_rain)
    hourly_data["snowfall"] = list(hourly_snowfall)
    hourly_data["cloud_cover"] = list(hourly_cloud_cover)

    return hourly_data