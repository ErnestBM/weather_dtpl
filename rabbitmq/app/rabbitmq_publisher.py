import pika
import logging
from datetime import datetime
from time import sleep
from fetcher import fetch_all_weather_data

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

RABBITMQ_QUEUE = "weather_data_v1"

DUMMY_WEATHER_DATA = {
    "coord": {"lon": 10.99, "lat": 44.34},
    "weather": [
        {"id": 804, "main": "Clouds", "description": "overcast clouds", "icon": "04n"}
    ],
    "base": "stations",
    "main": {
        "temp": 282.64,
        "feels_like": 281.31,
        "temp_min": 279.8,
        "temp_max": 282.64,
        "pressure": 1004,
        "humidity": 89,
        "sea_level": 1004,
        "grnd_level": 937,
    },
    "visibility": 10000,
    "wind": {"speed": 2.59, "deg": 192, "gust": 6.7},
    "clouds": {"all": 100},
    "dt": 1734637891,
    "sys": {
        "type": 2,
        "id": 2075663,
        "country": "IT",
        "sunrise": 1734590874,
        "sunset": 1734622720,
    },
    "timezone": 3600,
    "id": 3163858,
    "name": "Zocca",
    "cod": 200,
}

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
            # locations = {
            #     "Kretek": ("-7.9923", "110.2973"),
            #     "Jogjakarta": ("-7.8021", "110.3628"),
            #     "Menggoran": ("-7.9525", "110.4942"),
            #     "Bandara_DIY": ("-7.9007", "110.0573"),
            #     "Bantul": ("-7.8750", "110.3268"),
            # }
            # weather_data_list = fetch_all_weather_data(locations)

            # for weather_data in weather_data_list:
            #     channel.basic_publish(
            #         exchange='',
            #         routing_key=RABBITMQ_QUEUE,
            #         body=str(weather_data),
            #         properties=pika.BasicProperties(delivery_mode=2), 
            #     )
            #     logger.info(f" [x] Sent data: {weather_data}")

            # sleep(1) 

            response_json = DUMMY_WEATHER_DATA.copy()
            response_json["dt"] = int(datetime.now().timestamp())
            response_json["raw_produce_dt"] = int(datetime.now().timestamp() * 1_000_000)
            channel.basic_publish(
                    exchange='',
                    routing_key=RABBITMQ_QUEUE,
                    body=str(response_json),
                    properties=pika.BasicProperties(delivery_mode=2), 
            )
    except KeyboardInterrupt:
        logger.info(" [x] Stopping producer...")
    finally:
        connection.close()

if __name__ == "__main__":
    start_producer()