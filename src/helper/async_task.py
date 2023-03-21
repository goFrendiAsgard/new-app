from typing import Awaitable
import asyncio
import logging
import sys


def create_critical_task(
    logger: logging.Logger, awaitable: Awaitable
) -> asyncio.Task:
    async def critical_task(awaitable):
        try:
            return await awaitable
        except Exception as error:
            logger.critical(error)
            sys.exit(1)
    return asyncio.create_task(critical_task(awaitable))
