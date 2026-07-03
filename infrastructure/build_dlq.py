# build_dlq.py
import asyncio
import aio_pika

RABBITMQ_URL = "amqp://guest:guest@localhost:5672/"

async def setup_dlq():
    print("Connecting to RabbitMQ to build DLQ...")
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    
    async with connection:
        channel = await connection.channel()
        
        # 1. Create the Dead Letter Exchange
        dlx_bus = await channel.declare_exchange("dlx_bus", aio_pika.ExchangeType.DIRECT)
        
        # 2. Create the Dead Letter Queue
        dlq_queue = await channel.declare_queue("dead_letter_queue", durable=True)
        
        # 3. Bind them together
        await dlq_queue.bind(exchange="dlx_bus", routing_key="dlq.fatal")
        
        print("[v] Successfully provisioned the DLQ topology!")

if __name__ == "__main__":
    asyncio.run(setup_dlq())