import asyncio

import aio_pika


async def connect_rabbitmq(url: str, attempts: int = 15, base_delay: float = 2.0):
    last_error = None

    for attempt in range(1, attempts + 1):
        try:
            return await aio_pika.connect_robust(url)
        except Exception as error:
            last_error = error
            if attempt == attempts:
                break
            await asyncio.sleep(base_delay * attempt)

    raise last_error