import pika
import logging
import json
from mongodb_writer import write_to_mongo 

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

QUEUE_NAME = "processed_weather_data_v2"

def start_mongo_writer():
    """
    Consume messages from RabbitMQ queue and write them to MongoDB.
    """
    connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
    channel = connection.channel()

    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    logger.info(" [*] MongoDB Writer connected to RabbitMQ.")

    def callback(ch, method, properties, body):
        """
        Callback function to write messages to MongoDB.
        """
        logger.info(f" [x] Received data for MongoDB: {body}")
        try:
            message = json.loads(body.decode())
            write_to_mongo(message)
            logger.info(f" [x] Written to MongoDB: {message}")

            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            logger.error(f"Error writing to MongoDB: {e}")

            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)

    try:
        logger.info(" [*] Waiting for messages. To exit press CTRL+C")
        channel.start_consuming()
    except KeyboardInterrupt:
        logger.info(" [x] Stopping MongoDB writer...")
    finally:
        connection.close()

if __name__ == "__main__":
    start_mongo_writer()
