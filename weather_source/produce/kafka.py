import json
import math
import random
import time
import csv
import requests
import logging

from datetime import (
    datetime, 
    timedelta
)

from config.logging import Logger

from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(level=logging.DEBUG) 
logger = logging.getLogger(__name__)


class Producer:
    """
    Creates an instance of KafkaProducer with additional methods to produce dummy data.
    """
    def __init__(self, kafka_broker: str, kafka_topic: str) -> None:
        self._kafka_server = kafka_broker
        self._kafka_topic = kafka_topic
        self._instance = None
        self.logger = Logger().setup_logger(service_name='producer')
    

    def create_instance(self) -> KafkaProducer:
        """
        Creates new kafka producer and returns an instance of KafkaProducer.
        """
        self.logger.info(" [*] Starting Kafka producer...")
        self._instance = KafkaProducer(
            bootstrap_servers=self._kafka_server,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            api_version=(0,11,5)
        )
        return self._instance

    def is_kafka_connected(self) -> bool:
        """
        Check if the Kafka cluster is available by fetching metadata.
        """
        try:
            metadata = self._instance.bootstrap_connected()
            if metadata:
                self.logger.info(" [*] Connected to Kafka cluster successfully!")
                return True
            else:
                self.logger.error(" [X] Failed to connect to Kafka cluster.")
                return False
        except KafkaError as e:
            self.logger.error(f" [X] Kafka connection error: {e}")
            return False
        
    def ensure_bytes(self, message) -> bytes:
        """
        Ensure the message is in byte format.
        """
        if not isinstance(message, bytes):
            return bytes(message, encoding='utf-8')
        return message
    
    def produce(self) -> None:
        """
        Produces messages from a CSV file, simulating real-time data.
        """
        try:
            self.logger.info(" [*] Starting real-time Kafka producer.")
            while True:
                logger.debug("===debug=== Attempting to fetch weather data from OpenWeather API.")

                url = f"https://api.openweathermap.org/data/2.5/weather?lat=-6.2146&lon=106.8451&appid=7794c2f0e827d159325b614c8b7945a5"
                try:
                    response = requests.get(url)
                    response.raise_for_status() 
                    response_json = response.json()
                    logger.debug(f"Fetched weather data: {response_json}")

                    self._instance.send(self._kafka_topic, value=json.dumps(response_json))
                    logger.debug(f"Sent weather data to Kafka topic: {self._kafka_topic}")
                    
                except requests.exceptions.RequestException as e:
                    logger.error(f"Error fetching weather data: {e}")
                
                time.sleep(5)

        except Exception as e:
            pass
            self.logger.error(f" [X] {e}")
            self.logger.info(" [*] Stopping data generation.")
        finally:
            # close the kafka producer
            self._instance.close()