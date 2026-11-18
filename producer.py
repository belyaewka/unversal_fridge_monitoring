import asyncio
import aiomqtt
from loguru import logger
from typing import List

from config import MQTT_SERVER, MQTT_USER, MQTT_PASSWORD, MQTT_TOPICS, MESSAGE_QUEUE_MAX_SIZE, PRODUCER_LOGFILE

logger.add(sink=PRODUCER_LOGFILE, level='DEBUG')
# logger.add(PRODUCER_LOGFILE)


async def list_to_async_generator(collection):
    """utility make usual list asynchronous"""
    for item in collection:
        yield item


async def mqtt_producer(mqtt_broker: str,
                        user: str,
                        password: str,
                        topics: List[str],
                        queue: asyncio.Queue) -> None:
    """
    Receives messages from list of topics subscribed, put messages to async queue (FIFO)
    """
    logger.debug('Program starting')

    while True:
        try:
            logger.info(f"Starting MQTT producer, connecting to MQTT broker...")
            async with aiomqtt.Client(
                    hostname=mqtt_broker,
                    username=user,
                    password=password.encode('utf-8')) as client:
                logger.info(f"Client is connected. Subscribing to topics...")

                # make list of topics asynchronous
                topic_generator = list_to_async_generator(topics)

                # subscribing
                async for topic in topic_generator:
                    await client.subscribe(topic)
                    logger.info(f"Subscription is issued for topic {topic}")

                # Single message stream from all topics subscribed
                async for message in client.messages():
                    # print(message.payload)
                    # TODO made one unified format for mqtt messages, json format.
                    # TODO something like this
                    #  {"name": "Liebherr 4140",
                    #  "serial_number": "L8u80938927867",
                    #  "temperature": 4.5}
                    # msg = MqttMessage(
                    #     topic=str(message.topic),
                    #     payload=message.payload.decode("utf-8", errors="replace"),
                    #     timestamp=time.time()
                    # )

                    # Backpressure: memory control, if queue is full, waiting.
                    await queue.put(message.payload)
                    print(queue.qsize())
                    print(queue)

        except Exception as e:
            logger.error(f"MQTT error {e}. Reconnect in 5 sec...")
            await asyncio.sleep(5)


if __name__ == '__main__':
    client_queue = asyncio.Queue(maxsize=MESSAGE_QUEUE_MAX_SIZE)

    asyncio.run(mqtt_producer(mqtt_broker=MQTT_SERVER,
                              topics=MQTT_TOPICS,
                              user=MQTT_USER,
                              password=MQTT_PASSWORD,
                              queue=client_queue))
