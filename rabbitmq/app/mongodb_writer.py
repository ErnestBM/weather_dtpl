from pymongo import MongoClient
import os
import logging

MONGODB_URI = os.environ['MONGODB_URI']
MONGODB_DB = os.environ['MONGODB_DB']
MONGODB_COLLECTION = os.environ['MONGODB_COLLECTION']

def write_to_mongo(data: dict) -> None:
    """
    Writes a document to MongoDB.
    """
    try:
        client = MongoClient(MONGODB_URI)
        db = client[MONGODB_DB]
        collection = db[MONGODB_COLLECTION]
        collection.insert_one(data)
        logging.info(f"Inserted data into MongoDB: {data}")
    except Exception as e:
        logging.error(f"Error inserting data into MongoDB: {e}")
