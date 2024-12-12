import json
from kafka import KafkaConsumer
from kafka.errors import KafkaError # type: ignore
from config.logging import Logger
from config.utils import get_env_value

class Consumer:
    """
    Creates an instance of KafkaConsumer with additional methods to consume data.
    """
    def __init__(self, kafka_broker: str, kafka_topic: str, kafka_consumer_group: str) -> None:
        self._kafka_server = kafka_broker
        self._kafka_topic = kafka_topic
        self._kafka_consumer_group = kafka_consumer_group
        self._instance = None
        self.logger = Logger().setup_logger(service_name='consumer')
    
    def create_instance(self) -> KafkaConsumer: # type: ignore
        """
        Creates new kafka consumer and returns an instance of KafkaConsumer.
        """
        self._instance = KafkaConsumer(
            self._kafka_topic,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            bootstrap_servers=self._kafka_server,
            group_id=self._kafka_consumer_group,
            api_version=(0,9)
        ) # type: ignore
        return self._instance
    
    def is_kafka_connected(self) -> bool:
        """
        Check if the Kafka cluster is available by fetching metadata.
        """
        try:
            metadata = self._instance.bootstrap_connected() # type: ignore
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
        Consume messages indefinitely.
        """
        self.logger.info(" [*] Starting Kafka consumer...")
        try:
            for message in self._instance: # type: ignore
                self.logger.info(f" [*] Received message: {message.value}")

        except Exception as e:
            self.logger.error(f" [x] Failed to consume message: {e}")
            self.logger.info(" [*] Stopping Kafka consumer...")
        finally:
            self._instance.close() # type: ignore