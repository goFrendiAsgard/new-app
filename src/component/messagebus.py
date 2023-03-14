from config import (
    app_name, app_broker_type, app_rmq_connection, app_kafka_bootstrap_servers
)
from core.messagebus.messagebus import (
    Publisher, Consumer, MessageSerializer
)
from core.messagebus.rabbitmq import (
    RMQPublishConnection, RMQConsumeConnection, RMQConsumer, RMQPublisher
)
from core.messagebus.kafka import (
    KafkaConsumeConnection, KafkaPublishConnection,
    KafkaConsumer, KafkaPublisher
)
from component.log import logger


def init_serializer() -> MessageSerializer:
    return MessageSerializer()


def init_publisher(
    broker_type: str, serializer: MessageSerializer
) -> Publisher:
    if broker_type == 'rabbitmq':
        publish_connection = RMQPublishConnection(
            logger=logger, connection_string=app_rmq_connection
        )
        return RMQPublisher(
            logger=logger,
            publish_connection=publish_connection,
            serializer=serializer
        )
    if broker_type == 'kafka':
        publish_connection = KafkaPublishConnection(
            logger=logger, connection_string=app_kafka_bootstrap_servers
        )
        return KafkaPublisher(
            logger=logger,
            publish_connection=publish_connection,
            serializer=serializer
        )
    raise Exception(f'Invalid broker type: {broker_type}')


def init_consumer(
    broker_type: str, serializer: MessageSerializer
) -> Consumer:
    if broker_type == 'rabbitmq':
        consume_connection = RMQConsumeConnection(
            logger=logger, connection_string=app_rmq_connection
        )
        return RMQConsumer(
            logger=logger,
            consume_connection=consume_connection,
            serializer=serializer
        )
    if broker_type == 'kafka':
        consume_connection = KafkaConsumeConnection(
            logger=logger,
            connection_string=app_kafka_bootstrap_servers,
            group_id=app_name
        )
        return KafkaConsumer(
            logger=logger,
            consume_connection=consume_connection,
            serializer=serializer
        )
    raise Exception(f'Invalid broker type: {broker_type}')


serializer = init_serializer()
publisher = init_publisher(app_broker_type, serializer)
consumer = init_consumer(app_broker_type, serializer)


def get_consumer() -> Consumer:
    return consumer


def init_publisher() -> Publisher:
    return publisher
