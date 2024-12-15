import threading
from dotenv import load_dotenv # type: ignore
from fastapi import FastAPI # type: ignore
from process.kafka import Consumer, Producer
from config.utils import get_env_value

def consume_and_process(consumer: Consumer) -> None:
    """
    Run consumer instance with processing capabilities.
    """
    try:
        consumer.create_instance()
        consumer.consume()
    except KeyboardInterrupt:
        consumer.logger.info(" [*] Stopping Kafka consumer...")
        exit(1)

load_dotenv()

kafka_broker = get_env_value('KAFKA_BROKER')
kafka_consume_topic = get_env_value('KAFKA_CONSUME_TOPIC')
kafka_produce_topic = get_env_value('KAFKA_PRODUCE_TOPIC')
kafka_consumer_group = get_env_value('KAFKA_CONSUMER_GROUP')

producer = Producer(
    kafka_broker=kafka_broker, # type: ignore
    kafka_topic=kafka_produce_topic # type: ignore
)

consumer = Consumer(
    kafka_broker=kafka_broker, # type: ignore
    kafka_topic=kafka_consume_topic, # type: ignore
    kafka_consumer_group=kafka_consumer_group, # type: ignore
    producer=producer 
)

app = FastAPI()

@app.get("/ping")
async def healthcheck():
    return { "status": "healthy" }

consumer.logger.info(' [*] Before starting the threads.')

t_consumer = threading.Thread(
    target=consume_and_process,
    args=(consumer,),
    daemon=True
)

consumer.logger.info(f' [*] Healthcheck running on port 8000.')
t_consumer.start()
