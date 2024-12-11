from celery import Celery
from produce.fetch_weather import fetch_weather_data_srg, fetch_weather_data_jkt, fetch_weather_data_bdg, fetch_weather_data_smr, fetch_weather_data_sby
from datetime import datetime
from pymongo import MongoClient
import json
import os


celery_app = Celery("tasks", broker=os.getenv("CELERY_BROKER_URL"), include=["produce.celery"])

celery_app.autodiscover_tasks([])

celery_app.conf.broker_connection_retry_on_startup = True

client = MongoClient(os.getenv("MONGO_URI"))

db = client.openweather_db

collection = db.weather_records

celery_app.conf.beat_schedule = {
    "fetch-weather-srg-every-5-minutes": {
        "task": "produce.celery.scheduled_fetch_and_save_srg",
        "schedule": 5.0,  
    },
    "fetch-weather-jkt-every-5-minutes": {
        "task": "produce.celery.scheduled_fetch_and_save_jkt",
        "schedule": 5.0,  
    },
    "fetch-weather-bdg-every-5-minutes": {
        "task": "produce.celery.scheduled_fetch_and_save_bdg",
        "schedule": 5.0,  
    },
    "fetch-weather-smr-every-5-minutes": {
        "task": "produce.celery.scheduled_fetch_and_save_smr",
        "schedule": 5.0,  
    },
    "fetch-weather-sby-every-5-minutes": {
        "task": "produce.celery.scheduled_fetch_and_save_sby",
        "schedule": 5.0,  
    },
}

def save_to_mongodb(data):
    try:
        print("Data before saving to MongoDB:", json.dumps(data, indent=4))
        collection.insert_one(data)
        print("Data inserted successfully")
    except Exception as e:
        print(f"Error inserting data into MongoDB: {e}")

@celery_app.task()
def scheduled_fetch_and_save_srg():
    weather_data = fetch_weather_data_srg()
    if weather_data:
        weather_data["timestamp"] = datetime.now().isoformat()
        save_to_mongodb(weather_data)
    else:
        print("Failed to fetch Serang weather data")

@celery_app.task()
def scheduled_fetch_and_save_jkt():
    weather_data = fetch_weather_data_jkt()
    if weather_data:
        weather_data["timestamp"] = datetime.now().isoformat()
        save_to_mongodb(weather_data)
    else:
        print("Failed to fetch Jakarta weather data")

@celery_app.task()
def scheduled_fetch_and_save_bdg():
    weather_data = fetch_weather_data_bdg()
    if weather_data:
        weather_data["timestamp"] = datetime.now().isoformat()
        save_to_mongodb(weather_data)
    else:
        print("Failed to fetch Bandung weather data")

@celery_app.task()
def scheduled_fetch_and_save_smr():
    weather_data = fetch_weather_data_smr()
    if weather_data:
        weather_data["timestamp"] = datetime.now().isoformat()
        save_to_mongodb(weather_data)
    else:
        print("Failed to fetch Semarang weather data")

@celery_app.task()
def scheduled_fetch_and_save_sby():
    weather_data = fetch_weather_data_sby()
    if weather_data:
        weather_data["timestamp"] = datetime.now().isoformat()
        save_to_mongodb(weather_data)
    else:
        print("Failed to fetch Surabaya weather data")