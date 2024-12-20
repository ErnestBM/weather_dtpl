# import pika
# import logging
# import json
# from datetime import datetime
# from flatten import flatten_json
# from mongodb_writer import write_to_mongo

# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# RABBITMQ_QUEUE = "weather_data_v1"

# def start_consumer():
#     """
#     Consume messages from RabbitMQ queue and process them.
#     """
#     connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
#     channel = connection.channel()

#     channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
#     logger.info(" [*] Consumer connected to RabbitMQ.")

#     def callback(ch, method, properties, body):
#         """
#         Callback function to process received messages.
#         """
#         logger.info(f" [x] Received data: {body}")
#         try:
#             message_str = body.decode().replace("'", '"')

#             message = json.loads(message_str)
            
#             flat_data = flatten_json(message)

#             flat_data['process_dt'] = int(datetime.now().timestamp() * 1_000_000)

#             write_to_mongo(flat_data)

#             logger.info(f" [x] Processed and written to MongoDB: {flat_data}")
            
#             ch.basic_ack(delivery_tag=method.delivery_tag)
#         except Exception as e:
#             logger.error(f"Error processing message: {e}")

#             ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

#     channel.basic_consume(queue=RABBITMQ_QUEUE, on_message_callback=callback)

#     try:
#         logger.info(" [*] Waiting for messages. To exit press CTRL+C")
#         channel.start_consuming()
#     except KeyboardInterrupt:
#         logger.info(" [x] Stopping consumer...")
#     finally:
#         connection.close()

# if __name__ == "__main__":
#     start_consumer()

import pika
import logging
import json
from datetime import datetime
from flatten import flatten_json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

INPUT_QUEUE = "weather_data_latency_v1"
OUTPUT_QUEUE = "processed_weather_data_latency_v1"

def start_producer_consumer():
    """
    Consume messages from RabbitMQ input queue, process them, and produce them to another queue.
    """
    connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
    channel = connection.channel()

    channel.queue_declare(queue=INPUT_QUEUE, durable=True)
    channel.queue_declare(queue=OUTPUT_QUEUE, durable=True)

    logger.info(" [*] Producer-Consumer connected to RabbitMQ.")

    def callback(ch, method, properties, body):
        """
        Callback function to process received messages.
        """
        logger.info(f" [x] Received data: {body}")
        try:
            message_str = body.decode().replace("'", '"')
            message = json.loads(message_str)
            flat_data = flatten_json(message)

            flat_data['process_dt'] = int(datetime.now().timestamp() * 1_000_000)

            channel.basic_publish(
                exchange="",
                routing_key=OUTPUT_QUEUE,
                body=json.dumps(flat_data),
                properties=pika.BasicProperties(delivery_mode=2)  
            )
            logger.info(f" [x] Produced to {OUTPUT_QUEUE}: {flat_data}")

            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            logger.error(f"Error processing message: {e}")

            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    channel.basic_consume(queue=INPUT_QUEUE, on_message_callback=callback)

    try:
        logger.info(" [*] Waiting for messages. To exit press CTRL+C")
        channel.start_consuming()
    except KeyboardInterrupt:
        logger.info(" [x] Stopping producer-consumer...")
    finally:
        connection.close()

if __name__ == "__main__":
    start_producer_consumer()

