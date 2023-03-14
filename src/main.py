from component.messagebus import publisher, consumer
from component.app import app
from component.log import logger
from config import app_host, app_port, app_reload
import asyncio
import logging
import uvicorn

messages = []


@app.on_event('startup')
async def startup_event():
    logger.info('Started')
    asyncio.create_task(consumer.run())


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
    logging.info(f'Run on {app_host}:{app_port}')
    uvicorn.run("main:app", host=app_host, port=app_port, reload=app_reload, workers=1)
