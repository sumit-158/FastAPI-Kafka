import asyncio
from fastapi import FastAPI
from app.core.kafka_producer import kafkaProducer
from app.core.kafka_consumer import KafkaConsumer


loop = asyncio.get_event_loop()
kafka_consumer = None


app = FastAPI()

@app.on_event("startup")
async def startup():
    consume_kafka()

def consume_kafka():
    global kafka_consumer
    kafka_consumer = KafkaConsumer(loop)
    asyncio.create_task(kafka_consumer.consume())

@app.get("/producer")
async def root():
    await kafkaProducer.produce(topic="test", value={"phone_number": "some_phone_number"})
