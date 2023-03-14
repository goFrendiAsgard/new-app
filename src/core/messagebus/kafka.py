from typing import Any, Callable, Mapping, Optional
from core.messagebus.messagebus import (
    Publisher, Consumer, THandler, MessageSerializer,
    get_default_message_serializer
)
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

import asyncio
import inspect
import logging


class KafkaPublishConnection():
    def __init__(self, logger: logging.Logger, connection_string: str):
        self.logger = logger
        self.connection_string = connection_string
        self.producer: Optional[AIOKafkaProducer] = None

    async def __aenter__(self) -> AIOKafkaProducer:
        self.logger.info('🐼 Create kafka producer')
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.connection_string
        )
        self.logger.info('🐼 Start kafka producer')
        await self.producer.start()
        self.logger.info('🐼 Kafka producer started')
        return self.producer

    async def __aexit__(self, exc_type, exc, tb):
        self.logger.info('🐼 Stop kafka producer')
        await self.producer.stop()
        self.logger.info('🐼 Kafka producer stopped')


class KafkaPublisher(Publisher):
    def __init__(
        self,
        logger: logging.Logger,
        publish_connection: KafkaPublishConnection,
        serializer: Optional[MessageSerializer] = None,
        retry: int = 3
    ):
        self.logger = logger
        self.serializer = get_default_message_serializer(serializer)
        self.connection = publish_connection
        self._retry = retry

    async def publish(self, event_name: str, message: Any):
        return await self._publish(event_name, message, self._retry)

    async def _publish(self, event_name: str, message: Any, retry: int):
        try:
            async with self.connection as publisher:
                encoded_value = self.serializer.encode(event_name, message)
                self.logger.info(
                    f'🐼 Publish "{event_name}": {message}'
                )
                await publisher.send_and_wait(event_name, encoded_value)
            retry = self._retry
        except Exception:
            if retry == 0:
                raise
            await self._publish(event_name, message, retry-1)


class KafkaConsumeConnection():
    def __init__(
        self,
        logger: logging.Logger,
        connection_string: str,
        group_id: Optional[str] = None
    ):
        self.logger = logger
        self.connection_string = connection_string
        self.consumer: Optional[AIOKafkaConsumer] = None
        self.group_id = group_id

    async def __aenter__(self) -> AIOKafkaConsumer:
        self.logger.info('🐼 Create kafka consumer')
        self.consumer = AIOKafkaConsumer(
            bootstrap_servers=self.connection_string,
            group_id=self.group_id,
        )
        self.logger.info('🐼 Start kafka consumer')
        await self.consumer.start()
        self.logger.info('🐼 Kafka consumer started')
        return self.consumer

    async def __aexit__(self, exc_type, exc, tb):
        self.logger.info('🐼 Unsubscribe kafka consumer from all topics')
        self.consumer.unsubscribe()
        self.logger.info('🐼 Stop kafka consumer')
        await self.consumer.stop()
        self.logger.info('🐼 Kafka consumer stopped')


class KafkaConsumer(Consumer):
    def __init__(
        self,
        logger: logging.Logger,
        consume_connection: KafkaConsumeConnection,
        serializer: Optional[MessageSerializer] = None,
        retry: int = 5
    ):
        self.logger = logger
        self.serializer = get_default_message_serializer(serializer)
        self.connection = consume_connection
        self._handlers: Mapping[str, THandler] = {}
        self._retry = retry

    def register(self, event_name: str) -> Callable[[THandler], Any]:
        def wrapper(handler: THandler):
            self.logger.warning(f'🐼 Register handler for "{event_name}"')
            self._handlers[event_name] = handler
            return handler
        return wrapper

    async def run(self):
        return await self._run(self._retry)

    async def _run(self, retry: int):
        try:
            async with self.connection as consumer:
                topics = list(self._handlers.keys())
                self.logger.warning(f'🐼 Subscribe to topics: {topics}')
                consumer.subscribe(topics=topics)
                async for message in consumer:
                    event_name = message.topic
                    message_handler = self._handlers.get(event_name)
                    decoded_value = self.serializer.decode(
                        event_name, message.value
                    )
                    self.logger.info(
                        f'🐼 Consume "{event_name}": {decoded_value}'
                    )
                    await self._run_handler(message_handler, decoded_value)
                retry = self._retry
        except Exception:
            if retry == 0:
                self.logger.fatal('🐼 Cannot retry')
                raise
            self.logger.warning('🐼 Retry to consume')
            await self._run(retry-1)

    async def _run_handler(
        self, message_handler: THandler, decoded_value: Any
    ):
        if inspect.iscoroutinefunction(message_handler):
            return asyncio.create_task(message_handler(decoded_value))
        return message_handler(decoded_value)
