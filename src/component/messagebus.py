from config import (
    app_broker_type, app_rmq_connection, app_kafka_bootstrap_servers
)
from core.messagebus.messagebus import (
    Publisher, Consumer, MessageSerializer
)
from core.messagebus.rabbitmq import RMQConnection, RMQConsumer, RMQPublisher
from core.messagebus.kafka import (
    KafkaConsumeConnection, KafkaPublishConnection,
    KafkaConsumer, KafkaPublisher
)

publisher: Publisher
consumer: Consumer
serializer: MessageSerializer = MessageSerializer()

if app_broker_type == 'rabbitmq':
    connection = RMQConnection(app_rmq_connection)
    publisher = RMQPublisher(connection=connection, serializer=serializer)
    consumer = RMQConsumer(connection=connection, serializer=serializer)
elif app_broker_type == 'kafka':
    publish_connection = KafkaPublishConnection(app_kafka_bootstrap_servers)
    publisher = KafkaPublisher(
        publish_connection=publish_connection, serializer=serializer
    )
    consume_connection = KafkaConsumeConnection(app_kafka_bootstrap_servers)
    consumer = KafkaConsumer(
        consume_connection=consume_connection, serializer=serializer
    )


def get_consumer() -> Consumer:
    return consumer


def get_publisher() -> Publisher:
    return publisher