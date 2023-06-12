import logging
import os

from aiokafka import AIOKafkaConsumer

# Logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

BOOT_STRAP_SERVER = os.environ.get("BOOT_STRAP_SERVER")


class KafkaConsumer:
    """
    Kafka Consumer class to consume messages from Kafka topic
    """

    def __init__(self, loop):
        self.consumer = AIOKafkaConsumer(
            "test",
            loop=loop,
            bootstrap_servers=BOOT_STRAP_SERVER,
            group_id="test",
        )

    async def consume(self):
        """
        Consume messages from Kafka topic and create users based on the received data.
        """
        try:
            await self.consumer.start()

        except Exception as e:
            logging.info(str(e))
            return

        try:
            async for msg in self.consumer:
                msg.value.decode("utf-8")  # Decode byte object to string

        finally:
            await self.consumer.stop()
