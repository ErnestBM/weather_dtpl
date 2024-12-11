import requests
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("OPENWEATHER_API_KEY")

LAT_SRG = os.getenv("LATITUDE_SMG", "-6.1169")
LON_SRG = os.getenv("LONGITUDE_SMG", "106.1539")

LAT_JKT = os.getenv("LATITUDE_JKT", "-6.2146")
LON_JKT = os.getenv("LONGITUDE_JKT", "106.8451")

LAT_BDG = os.getenv("LATITUDE_SMG", "-6.9175")
LON_BDG = os.getenv("LONGITUDE_SMG", "107.6191")

LAT_SMR = os.getenv("LATITUDE_SMR", "-6.9932")
LON_SMR = os.getenv("LONGITUDE_SMR", "110.4203")

LAT_SBY = os.getenv("LATITUDE_SBY", "-7.2575")
LON_SBY = os.getenv("LONGITUDE_SBY", "112.7521")


def fetch_weather_data(lat: str, lon: str):
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching data: {response.status_code}")
        return None
    
def fetch_weather_data_srg():
    return fetch_weather_data(LAT_SRG, LON_SRG)

def fetch_weather_data_jkt():
    return fetch_weather_data(LAT_JKT, LON_JKT)

def fetch_weather_data_bdg():
    return fetch_weather_data(LAT_BDG, LON_BDG)

def fetch_weather_data_smr():
    return fetch_weather_data(LAT_SMR, LON_SMR)

def fetch_weather_data_sby():
    return fetch_weather_data(LAT_SBY, LON_SBY)