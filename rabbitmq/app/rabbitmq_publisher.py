import pika
import logging
import os
from time import sleep
from fetcher import fetch_all_weather_data

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

RABBITMQ_QUEUE = os.getenv('RABBITMQ_QUEUE')

def start_producer():
    """
    Fetch weather data and send it to RabbitMQ queue.
    """

    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
        channel = connection.channel()
        logger.info(" [*] Producer connected to RabbitMQ.")
    except Exception as e:
        logger.error(f"Failed to connect to RabbitMQ: {e}")
        raise

    channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
    logger.info(" [*] Producer connected to RabbitMQ.")

    try:
        while True:
            locations = {
                "Kretek": ("-7.9923", "110.2973"),
                "Jogjakarta": ("-7.8021", "110.3628"),
                "Menggoran": ("-7.9525", "110.4942"),
                "Bandara_DIY": ("-7.9007", "110.0573"),
                "Bantul": ("-7.8750", "110.3268"),
            }
            weather_data_list = fetch_all_weather_data(locations)

            for weather_data in weather_data_list:
                channel.basic_publish(
                    exchange='',
                    routing_key=RABBITMQ_QUEUE,
                    body=str(weather_data),
                    properties=pika.BasicProperties(delivery_mode=2), 
                )
                logger.info(f" [x] Sent data: {weather_data}")

            sleep(1) 
    except KeyboardInterrupt:
        logger.info(" [x] Stopping producer...")
    finally:
        connection.close()

if __name__ == "__main__":
    start_producer()