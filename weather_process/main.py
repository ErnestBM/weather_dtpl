import threading
from dotenv import load_dotenv # type: ignore
from fastapi import FastAPI # type: ignore
from process.kafka import Consumer
from config.utils import get_env_value

def consume(consumer: Consumer) -> None:
    """
    Run consumer instance.
    """
    try:
        consumer.create_instance()
        consumer.consume()
    except KeyboardInterrupt:
        consumer.logger.info(" [*] Stopping Kafka consumer...")
        exit(1)

load_dotenv()


consumer = Consumer(
    kafka_broker=get_env_value('KAFKA_BROKER'),  # type: ignore
    kafka_topic=get_env_value('KAFKA_TOPIC'), # type: ignore
    kafka_consumer_group=get_env_value('KAFKA_CONSUMER_GROUP'), # type: ignore
)

app = FastAPI()

@app.get("/ping")
async def healthcheck():
    return { "status": "healthy" }

consumer.logger.info(f' [*] Healthcheck running on port 8000.')

t_consumer = threading.Thread(
    target=consume,
    args=(consumer,),
    daemon=True
)
t_consumer.start()