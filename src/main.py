from component.app import app
from component.log import logger
from component.message_consumer import consumer
from component.message_publisher import publisher
from config import app_host, app_port, app_reload
from helper.async_task import create_critical_task
import uvicorn

messages = []


@app.on_event('startup')
async def startup_event():
    logger.info('Started')
    create_critical_task(logger, consumer.run())


@consumer.register('coba')
async def handle_event(message):
    messages.append(message)
    print(messages)


@app.get('/')
def handle_get():
    return ('hello world')


@app.get('/send')
async def handle_send():
    return await publisher.publish('coba', 'sesuatu')


if __name__ == "__main__":
    logger.info(f'Run on {app_host}:{app_port}')
    uvicorn.run("main:app", host=app_host, port=app_port, reload=app_reload)
