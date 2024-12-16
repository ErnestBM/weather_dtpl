import os
import logging
from fastapi import FastAPI # type: ignore
from write.kafka import Consumer
from threading import Thread
from dotenv import load_dotenv # type: ignore
from config.utils import get_env_value

load_dotenv()

def consume(consumer: Consumer) -> None:
    """
    Run Kafka Consumer instance.
    """
    try:
        consumer.create_instance()
        consumer.consume()
    except KeyboardInterrupt:
        exit(1)

app = FastAPI()

kafka_broker = get_env_value('KAFKA_BROKER')
kafka_topic = get_env_value('KAFKA_CONSUME_TOPIC')
kafka_consumer_group = get_env_value('KAFKA_CONSUMER_GROUP')
mongodb_uri = get_env_value('MONGODB_URI')
mongodb_db = get_env_value('MONGODB_DB')
mongodb_collection = get_env_value('MONGODB_COLLECTION')


consumer = Consumer(
    kafka_broker=kafka_broker, # type: ignore
    kafka_topic=kafka_topic, # type: ignore
    kafka_consumer_group=kafka_consumer_group, # type: ignore
    mongodb_uri=mongodb_uri, # type: ignore
    mongodb_db=mongodb_db, # type: ignore
    mongodb_collection=mongodb_collection # type: ignore
)

t_consumer = Thread(target=consume, args=(consumer,), daemon=True)
t_consumer.start()
