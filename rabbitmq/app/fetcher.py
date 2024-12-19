import requests
import os
import logging
from datetime import datetime
from typing import Dict

logging.basicConfig(level=logging.INFO)

API_KEY = os.getenv('OPENWEATHER_API_KEY')

locations = {
    "Kretek": ("-7.9923", "110.2973"),
    "Jogjakarta": ("-7.8021", "110.3628"),
    "Menggoran": ("-7.9525", "110.4942"),
    "Bandara_DIY": ("-7.9007", "110.0573"),
    "Bantul": ("-7.8750", "110.3268"),
}

def fetch_weather_data(lat: str, lon: str) -> dict:
    """
    Fetch weather data from OpenWeather API.
    """
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching weather data: {e}")
        return {}

def fetch_all_weather_data(locations: Dict[str, tuple]) -> list:
    """
    Fetch weather data for all specified locations.
    """
    all_weather_data = []
    for location, coords in locations.items():
        lat, lon = coords
        logging.info(f"Fetching weather data for {location} (lat: {lat}, lon: {lon})...")
        weather_data = fetch_weather_data(lat, lon)
        if weather_data:
            weather_data["location"] = location 
            weather_data["produce_dt"] = int(datetime.now().timestamp() * 1_000_000)

            all_weather_data.append(weather_data)
            logging.info(f"Successfully fetched data for {location}: {weather_data}")
        else:
            logging.warning(f"No data received for {location}.")
    return all_weather_data