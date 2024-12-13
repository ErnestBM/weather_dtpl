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

kafka_broker = get_env_value('KAFKA_BROKER')
kafka_topic = get_env_value('KAFKA_TOPIC')
kafka_consumer_group = get_env_value('KAFKA_CONSUMER_GROUP')

consumer = Consumer(
    kafka_broker=kafka_broker,  # type: ignore
    kafka_topic=kafka_topic, # type: ignore
    kafka_consumer_group=kafka_consumer_group, # type: ignore
)

app = FastAPI()

@app.get("/ping")
async def healthcheck():
    return { "status": "healthy" }

consumer.logger.info(' [*] Before starting the thread.')

# Start Kafka consumer in a separate thread
t_consumer = threading.Thread(
    target=consume,
    args=(consumer,),
    daemon=True
)

consumer.logger.info(f' [*] Healthcheck running on port 8000.')
t_consumer.start()
