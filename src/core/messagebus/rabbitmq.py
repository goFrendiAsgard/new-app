from typing import Any, Callable, Mapping, Optional
from core.messagebus.messagebus import (
    Publisher, Consumer, THandler, MessageSerializer,
    get_default_message_serializer
)
import asyncio
import aiormq


class RMQConnection():
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.connection: Optional[aiormq.Connection] = None

    async def __aenter__(self) -> aiormq.Connection:
        self.connection = await aiormq.connect(self.connection_string)
        return self.connection

    async def __aexit__(self, exc_type, exc, tb):
        await self.connection.close()


class RMQPublisher(Publisher):
    def __init__(
        self,
        connection: RMQConnection,
        serializer: Optional[MessageSerializer] = None,
        retry: int = 3
    ):
        self.serializer = get_default_message_serializer(serializer)
        self.connection = connection
        self._retry = retry

    async def publish(self, event_name: str, message: Any):
        return await self._publish(event_name, message, self._retry)

    async def _publish(self, event_name: str, message: Any, retry: int):
        try:
            async with self.connection as conn:
                channel = await conn.channel()
                await channel.queue_declare(event_name)
                await channel.basic_publish(
                    body=self.serializer.encode(event_name, message),
                    routing_key=event_name,
                )
            retry = self._retry
        except Exception:
            if retry == 0:
                raise
            await self._publish(event_name, message, retry-1)


class RMQConsumer(Consumer):
    def __init__(
        self,
        connection: RMQConnection,
        serializer: Optional[MessageSerializer] = None,
        retry: int = 3
    ):
        self.serializer = get_default_message_serializer(serializer)
        self.connection = connection
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
            async with self.connection as conn:
                channel = await conn.channel()
                for event_name, handler in self._handlers.items():
                    await channel.queue_declare(event_name)
                    message_handler = handler
                    on_message = self._create_consumer_callback(
                        channel, event_name, message_handler
                    )
                    asyncio.create_task(channel.basic_consume(
                        queue=event_name, consumer_callback=on_message
                    ))
            retry = self._retry
        except Exception:
            if retry == 0:
                raise
            await self._run(retry-1)

    def _create_consumer_callback(
        self,
        channel: aiormq.Channel,
        event_name: str,
        message_handler: THandler
    ) -> Callable[[Any], Any]:
        async def on_message(message):
            message_handler(
                self.serializer.decode(event_name, message.body)
            )
            await channel.basic_ack(message.delivery_tag)
        return on_message
