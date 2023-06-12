import json
import logging
import os

from aiokafka import AIOKafkaProducer

# Logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
BOOT_STRAP_SERVER = os.environ.get("BOOT_STRAP_SERVER")


class kafkaProducer:
    async def produce(topic, value):
        try:
            producer = AIOKafkaProducer(bootstrap_servers=BOOT_STRAP_SERVER)
            await producer.start()

            try:
                await producer.send_and_wait(topic, json.dumps(value).encode())
            finally:
                await producer.stop()

        except Exception as err:
            logging.log(f"Some Kafka error: {err}")
