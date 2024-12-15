import json
import csv
import io
import logging
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError  # type: ignore
from config.logging import Logger

logging.basicConfig(level=logging.DEBUG) 
logger = logging.getLogger(__name__)

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
    
    def create_instance(self) -> KafkaConsumer:  # type: ignore
        """
        Creates new Kafka consumer and returns an instance of KafkaConsumer.
        """
        self._instance = KafkaConsumer(
            self._kafka_topic,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            bootstrap_servers=self._kafka_server,
            group_id=self._kafka_consumer_group,
            api_version=(0,11,5)
        )  # type: ignore
        return self._instance
    
    def is_kafka_connected(self) -> bool:
        """
        Check if the Kafka cluster is available by fetching metadata.
        """
        try:
            metadata = self._instance.bootstrap_connected()  # type: ignore
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
            for message in self._instance:  # type: ignore
                self.logger.info(f" [*] Received message: {message.value}")
                logger.info(f" [*] Received message: {message.value}")

                csv_data = self.json_to_csv(message.value)

                logger.info(f" [*] Received message: {csv_data}")

                processed_message = {"csv_data": csv_data}
                self._producer.send_message(processed_message)

        except Exception as e:
            self.logger.error(f" [x] Failed to consume message: {e}")
            self.logger.info(" [*] Stopping Kafka consumer...")
        finally:
            self._instance.close()  # type: ignore

    def json_to_csv(self, json_data: dict) -> str:
        """
        Converts a JSON object to a CSV string.
        """
        csv_buffer = io.StringIO()
        csv_writer = csv.DictWriter(csv_buffer, fieldnames=json_data.keys())
        
        csv_writer.writeheader()
        csv_writer.writerow(json_data)
        
        csv_string = csv_buffer.getvalue()
        csv_buffer.close()
        
        self.logger.info(f" [*] Converted JSON to CSV:\n{csv_string}")
        return csv_string


class Producer:
    """
    Creates an instance of KafkaProducer and provides methods to send messages.
    """
    def __init__(self, kafka_broker: str, kafka_topic: str) -> None:
        self._kafka_server = kafka_broker
        self._kafka_topic = kafka_topic
        self._instance = None
        self.logger = Logger().setup_logger(service_name="producer")

        # Create KafkaProducer instance
        self._instance = KafkaProducer(  # type: ignore
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
            self._instance.send(self._kafka_topic, value=message)  # type: ignore
        except KafkaError as e:
            self.logger.error(f" [x] Failed to send message: {e}")
        finally:
            self._instance.flush()  # type: ignore
