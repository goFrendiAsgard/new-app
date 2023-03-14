from typing import Any, Callable, Mapping, Optional
from core.messagebus.messagebus import (
    Publisher, Consumer, THandler, MessageSerializer,
    get_default_message_serializer
)
import asyncio
import aiormq
import inspect
import logging


class RMQPublishConnection():
    def __init__(self, logger: logging.Logger, connection_string: str):
        self.logger = logger
        self.connection_string = connection_string
        self.connection: Optional[aiormq.Connection] = None

    async def __aenter__(self) -> aiormq.Connection:
        self.logger.info('🐰 Create publisher connection')
        self.connection = await aiormq.connect(self.connection_string)
        self.logger.info('🐰 Publisher connection created')
        return self.connection

    async def __aexit__(self, exc_type, exc, tb):
        self.logger.info('🐰 Close publisher connection')
        await self.connection.close()
        self.logger.info('🐰 Publisher connection closed')


class RMQPublisher(Publisher):
    def __init__(
        self,
        logger: logging.Logger,
        publish_connection: RMQPublishConnection,
        serializer: Optional[MessageSerializer] = None,
        retry: int = 5
    ):
        self.logger = logger
        self.serializer = get_default_message_serializer(serializer)
        self.connection = publish_connection
        self._retry = retry

    async def publish(self, event_name: str, message: Any):
        return await self._publish(event_name, message, self._retry)

    async def _publish(self, event_name: str, message: Any, retry: int):
        try:
            async with self.connection as conn:
                self.logger.info('🐰 Get channel')
                channel = await conn.channel()
                self.logger.info(f'🐰 Declare queue: {event_name}')
                await channel.queue_declare(event_name)
                self.logger.info(f'🐰 Publish "{event_name}": {message}')
                await channel.basic_publish(
                    body=self.serializer.encode(event_name, message),
                    routing_key=event_name,
                )
            retry = self._retry
        except Exception:
            if retry == 0:
                raise
            await self._publish(event_name, message, retry-1)


class RMQConsumeConnection():
    def __init__(self, logger: logging.Logger, connection_string: str):
        self.logger = logger
        self.connection_string = connection_string
        self.connection: Optional[aiormq.Connection] = None

    async def __aenter__(self) -> aiormq.Connection:
        self.logger.info('🐰 Create consumer connection')
        self.connection = await aiormq.connect(self.connection_string)
        self.logger.info('🐰 Consumer connection created')
        return self.connection

    async def __aexit__(self, exc_type, exc, tb):
        self.logger.info('🐰 Close consumer connection')
        await self.connection.close()
        self.logger.info('🐰 Consumer connection closed')


class RMQConsumer(Consumer):
    def __init__(
        self,
        logger: logging.Logger,
        consume_connection: RMQConsumeConnection,
        serializer: Optional[MessageSerializer] = None,
        retry: int = 5
    ):
        self.logger = logger
        self.connection = consume_connection
        self._handlers: Mapping[str, THandler] = {}
        self.serializer = get_default_message_serializer(serializer)
        self._retry = retry

    def register(self, event_name: str) -> Callable[[THandler], Any]:
        def wrapper(handler: THandler):
            self.logger.warning(f'🐰 Register handler for "{event_name}"')
            self._handlers[event_name] = handler
            return handler
        return wrapper

    async def run(self):
        return await self._run(self._retry)

    async def _run(self, retry: int):
        try:
            async with self.connection as conn:
                self.logger.info('🐰 Get channel')
                channel = await conn.channel()
                for event_name, handler in self._handlers.items():
                    self.logger.info(f'🐰 Declare queue: {event_name}')
                    await channel.queue_declare(event_name)
                    on_message = self._create_consumer_callback(
                        channel, event_name
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
    ) -> Callable[[Any], Any]:
        async def on_message(message):
            decoded_value = self.serializer.decode(event_name, message.body)
            handler = self._handlers.get(event_name)
            self.logger.info(f'🐰 Consume "{event_name}": {decoded_value}')
            await self._run_handler(handler, decoded_value)
            await channel.basic_ack(message.delivery_tag)
        return on_message

    async def _run_handler(
        self, message_handler: THandler, decoded_value: Any
    ):
        if inspect.iscoroutinefunction(message_handler):
            return asyncio.create_task(message_handler(decoded_value))
        return message_handler(decoded_value)
