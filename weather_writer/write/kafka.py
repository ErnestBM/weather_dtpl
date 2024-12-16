import os
import json
import logging
from kafka import KafkaConsumer
from config.logging import Logger
from kafka.errors import KafkaError # type: ignore
from pymongo import MongoClient  # type: ignore
from pymongo.errors import PyMongoError # type: ignore

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("writer_service")

class Consumer:
    """
    Kafka Consumer that consumes messages and writes them to MongoDB.
    """
    def __init__(self, kafka_broker: str, kafka_topic: str, kafka_consumer_group: str, mongodb_uri: str, mongodb_db: str, mongodb_collection: str) -> None:
        self._kafka_server = kafka_broker
        self._kafka_topic = kafka_topic
        self._kafka_consumer_group = kafka_consumer_group
        self._mongodb_uri = mongodb_uri
        self._mongodb_db = mongodb_db
        self._mongodb_collection = mongodb_collection
        self._instance = None

        try:
            logger.info(mongodb_uri + " debug22")
            logger.info(mongodb_db + " debug22")
            logger.info(mongodb_collection + " debug22")

            self._mongo_client = MongoClient(self._mongodb_uri)
            logger.info(mongodb_uri + " debug22")
            self._db = self._mongo_client[self._mongodb_db]
            self._collection = self._db[self._mongodb_collection]
            logger.info(" [*] Connected to MongoDB successfully.")
        except PyMongoError as e:
            logger.error(f" [X] Failed to connect to MongoDB: {e}")
            raise

    def create_instance(self) -> KafkaConsumer: # type: ignore
        """
        Creates a new KafkaConsumer instance.
        """
        self._instance = KafkaConsumer(
            self._kafka_topic,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            bootstrap_servers=self._kafka_server,
            group_id=self._kafka_consumer_group,
            api_version=(0, 11, 5)
        ) # type: ignore
        return self._instance

    def is_kafka_connected(self) -> bool:
        """
        Check if the Kafka cluster is available by fetching metadata.
        """
        try:
            metadata = self._instance.bootstrap_connected() # type: ignore
            if metadata:
                logger.info(" [*] Kafka connection OK.")
                return True
            else:
                logger.error(" [X] Kafka not connected!")
                return False
        except KafkaError as e:
            logger.error(f" [X] Kafka connection error!: {e}")
            return False

    def consume(self) -> None:
        """
        Consume messages from Kafka and write them to MongoDB.
        """
        logger.info(" [*] Starting Kafka consumer...")
        try:
            for message in self._instance: # type: ignore
                logger.info(f" [*] Received message: {message.value}")

                self.write_to_mongodb(message.value)

        except Exception as e:
            logger.error(f" [x] Failed to consume message: {e}")
            logger.info(" [*] Stopping Kafka consumer...")
        finally:
            self._instance.close() # type: ignore

    def write_to_mongodb(self, data: dict) -> None:
        """
        Writes the given data to MongoDB.
        """
        try:
            self._collection.insert_one(data)
            logger.info(" [*] Successfully wrote message to MongoDB.")
        except PyMongoError as e:
            logger.error(f" [x] Failed to write to MongoDB: {e}")