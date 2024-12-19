import json
import logging
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError 
from config.logging import Logger

logging.basicConfig(level=logging.DEBUG) 
logger = logging.getLogger(__name__)

def flatten_json(nested_json, parent_key='', sep='_'):
    """
    Flatten a nested JSON object into a flat dictionary, removing parent names from the keys.
    """
    items = {}
    for key, value in nested_json.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key 

        if isinstance(value, dict):
            items.update(flatten_json(value, new_key, sep))
        elif isinstance(value, list):
            for i, sub_item in enumerate(value):
                items.update(flatten_json(sub_item, f"{new_key}_{i}", sep))
        else:
            items[new_key] = value
    return items


class Consumer:
    """
    Creates an instance of KafkaConsumer with additional methods to consume data.
    """
    def __init__(self, kafka_broker: str, kafka_topic: str, kafka_consumer_group: str, producer: 'Producer') -> None:
        self._kafka_server = kafka_broker
        self._kafka_topic = kafka_topic
        self._kafka_consumer_group = kafka_consumer_group
        self._instance = None
        self._producer = producer 
        self.logger = Logger().setup_logger(service_name='consumer')
    
    def create_instance(self) -> KafkaConsumer:  
        """
        Creates new Kafka consumer and returns an instance of KafkaConsumer.
        """
        self._instance = KafkaConsumer(
            self._kafka_topic,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            bootstrap_servers=self._kafka_server,
            group_id=self._kafka_consumer_group,
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
                self.logger.info(" [*] Kafka connection OK.")
                return True
            else:
                self.logger.error(" [X] Kafka not connected!")
                return False
        except KafkaError as e:
            self.logger.error(f" [X] Kafka connection error!: {e}")
            return False
    
    def consume(self) -> None:
        """
        Consume messages, process them, and send the processed data to Kafka.
        """
        self.logger.info(" [*] Starting Kafka consumer...")
        try:
            for message in self._instance:  
                logger.info(f" [*] Received message: {message.value}")

                flattened_data = flatten_json(message.value)

                flattened_data['process_dt'] = int(datetime.now().timestamp() * 1_000_000)

                logger.info(f" [*] Received message: {flattened_data}")

                processed_message = flattened_data
                self._producer.send_message(processed_message)

        except Exception as e:
            self.logger.error(f" [x] Failed to consume message: {e}")
            self.logger.info(" [*] Stopping Kafka consumer...")
        finally:
            self._instance.close() 


class Producer:
    """
    Creates an instance of KafkaProducer and provides methods to send messages.
    """
    def __init__(self, kafka_broker: str, kafka_topic: str) -> None:
        self._kafka_server = kafka_broker
        self._kafka_topic = kafka_topic
        self._instance = None
        self.logger = Logger().setup_logger(service_name="producer")

        self._instance = KafkaProducer(  
            bootstrap_servers=self._kafka_server,
            api_version=(0, 11, 5),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

    def send_message(self, message: dict) -> None:
        """
        Send a message to Kafka.
        """
        try:
            self.logger.info(f" [*] Sending message to Kafka: {message}")
            self._instance.send(self._kafka_topic, value=message) 
        except KafkaError as e:
            self.logger.error(f" [x] Failed to send message: {e}")
        finally:
            self._instance.flush() 
