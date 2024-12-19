import pika
import logging
import json
import os
from flatten import flatten_json
from mongodb_writer import write_to_mongo

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

RABBITMQ_QUEUE = os.environ['RABBITMQ_QUEUE']

def start_consumer():
    """
    Consume messages from RabbitMQ queue and process them.
    """
    connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
    channel = connection.channel()

    channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
    logger.info(" [*] Consumer connected to RabbitMQ.")

    def callback(ch, method, body):
        """
        Callback function to process received messages.
        """
        logger.info(f" [x] Received data: {body}")
        try:
            message_str = body.decode().replace("'", '"')

            message = json.loads(message_str)
            
            flat_data = flatten_json(message)
            
            write_to_mongo(flat_data)

            logger.info(f" [x] Processed and written to MongoDB: {flat_data}")
            
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            logger.error(f"Error processing message: {e}")

            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    channel.basic_consume(queue=RABBITMQ_QUEUE, on_message_callback=callback)

    try:
        logger.info(" [*] Waiting for messages. To exit press CTRL+C")
        channel.start_consuming()
    except KeyboardInterrupt:
        logger.info(" [x] Stopping consumer...")
    finally:
        connection.close()

if __name__ == "__main__":
    start_consumer()
