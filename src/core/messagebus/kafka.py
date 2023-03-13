from typing import Any, Callable, Mapping, Optional
from core.messagebus.messagebus import (
    Publisher, Consumer, THandler, MessageSerializer,
    get_default_message_serializer
)
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import asyncio


class KafkaPublishConnection():
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.producer: Optional[AIOKafkaProducer] = None

    async def __aenter__(self) -> AIOKafkaProducer:
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.connection_string
        )
        await self.producer.start()
        return self.producer

    async def __aexit__(self, exc_type, exc, tb):
        await self.producer.stop()


class KafkaPublisher(Publisher):
    def __init__(
        self,
        publish_connection: KafkaPublishConnection,
        serializer: Optional[MessageSerializer] = None,
        retry: int = 3
    ):
        self.serializer = get_default_message_serializer(serializer)
        self.connection = publish_connection
        self._retry = retry

    async def publish(self, event_name: str, message: Any):
        return await self._publish(event_name, message, self._retry)

    async def _publish(self, event_name: str, message: Any, retry: int):
        try:
            async with self.connection as publisher:
                await publisher.send_and_wait(
                    event_name, self.serializer.encode(event_name, message)
                )
            retry = self._retry
        except Exception:
            if retry == 0:
                raise
            await self._publish(event_name, message, retry-1)


class KafkaConsumeConnection():
    def __init__(self, connection_string: str, group_id: Optional[str] = None):
        self.connection_string = connection_string
        self.consumer: Optional[AIOKafkaConsumer] = None
        self.group_id = group_id

    async def __aenter__(self) -> AIOKafkaConsumer:
        self.consumer = AIOKafkaConsumer(
            bootstrap_servers=self.connection_string,
            group_id=self.group_id,
            # consumer_timeout_ms=1000,
            # enable_auto_commit=False,
            # auto_offset_reset='earliest'
        )
        await self.consumer.start()
        return self.consumer

    async def __aexit__(self, exc_type, exc, tb):
        self.consumer.unsubscribe()
        await self.consumer.stop()


class KafkaConsumer(Consumer):
    def __init__(
        self,
        consume_connection: KafkaConsumeConnection,
        serializer: Optional[MessageSerializer] = None,
        retry: int = 5
    ):
        self.serializer = get_default_message_serializer(serializer)
        self.connection = consume_connection
        self._handlers: Mapping[str, THandler] = {}
        self._retry = retry

    def register(self, event_name: str) -> Callable[[THandler], Any]:
        def wrapper(handler: THandler):
            self._handlers[event_name] = handler
            return handler
        return wrapper

    async def run(self):
        return await self._run(self._retry)

    async def _run(self, retry: int):
        try:
            async with self.connection as consumer:
                topics = list(self._handlers.keys())
                consumer.subscribe(topics=topics)
                while True:
                    async for message in consumer:
                        event_name = message.topic
                        print(event_name, message)
                        handler = self._handlers.get(event_name)
                        if handler is None:
                            continue
                        message_handler = handler
                        asyncio.create_task(
                            message_handler(self.serializer.decode(
                                event_name, message.value
                            ))
                        )
                    # await asyncio.sleep(0.01)
                    retry = self._retry
        except Exception:
            if retry == 0:
                raise
            await self._run(retry-1)
