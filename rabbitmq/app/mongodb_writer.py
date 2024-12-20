from datetime import datetime
from pymongo import MongoClient
import os
import logging

MONGO_URI = 'mongodb://abby.marvel:SpeakLouder@data-station-mongo:27017'
DB_NAME = 'openweather_rabbitmq'
COLLECTION_NAME = 'weather_testing_2'

def write_to_mongo(data: dict) -> None:
    """
    Writes a document to MongoDB.
    """
    try:
        data['write_dt'] = int(datetime.now().timestamp() * 1_000_000)
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        collection.insert_one(data)
        logging.info(f"Inserted data into MongoDB: {data}")
    except Exception as e:
        logging.error(f"Error inserting data into MongoDB: {e}")
