from helper.conversion import str_to_boolean
import os

app_broker_type = os.environ.get('APP_BROKER_TYPE', 'rabbitmq')
app_host = os.environ.get('APP_HOST', '0.0.0.0')
app_port = int(os.environ.get('APP_PORT', '8080'))
app_reload = str_to_boolean(os.environ.get('APP_RELOAD', 'true'))

app_rmq_connection = os.environ.get(
    'APP_RMQ_CONNECTION', 'amqp://guest:guest@localhost/'
)

app_kafka_bootstrap_servers = os.environ.get(
    'APP_KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'
)
