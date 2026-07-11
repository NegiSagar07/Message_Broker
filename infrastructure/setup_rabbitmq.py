# infrastructure/setup_rabbitmq.py
import asyncio
import aio_pika

RABBITMQ_URL = "amqp://guest:guest@localhost:5672/"

async def main():
    print("Connecting to RabbitMQ to provision infrastructure...")
    
    connection = await aio_pika.connect_robust(RABBITMQ_URL)

    async with connection:
        channel = await connection.channel()

        # ---------------------------------------------------------
        # 1. DECLARE ALL EXCHANGES (The Intersections)
        # ---------------------------------------------------------
        raw_event_bus = await channel.declare_exchange("raw_event_bus", aio_pika.ExchangeType.TOPIC, durable=True)
        dispatch_bus = await channel.declare_exchange("dispatch_bus", aio_pika.ExchangeType.DIRECT, durable=True)
        delay_bus = await channel.declare_exchange("delay_bus", aio_pika.ExchangeType.DIRECT, durable=True)
        dlx_bus = await channel.declare_exchange("dlx_bus", aio_pika.ExchangeType.DIRECT, durable=True)

        # ---------------------------------------------------------
        # 2. DECLARE PRIMARY QUEUES (The Active Work)
        # ---------------------------------------------------------
        router_queue = await channel.declare_queue("router_queue", durable=True)
        await router_queue.bind(exchange=raw_event_bus, routing_key='#')

        primary_dispatch_queue = await channel.declare_queue("primary_dispatch_queue", durable=True)
        await primary_dispatch_queue.bind(exchange=dispatch_bus, routing_key='dispatch.execute')

        # ---------------------------------------------------------
        # 3. DECLARE THE TIME MACHINE (The TTL Retries)
        # ---------------------------------------------------------
        delay_1m_queue = await channel.declare_queue(
            name='delay_1m_queue',
            durable=True,
            arguments={
                "x-message-ttl": 60000,                         
                "x-dead-letter-exchange": "dispatch_bus",       
                "x-dead-letter-routing-key": "dispatch.execute" 
            }
        )
        await delay_1m_queue.bind(exchange=delay_bus, routing_key="delay.1m")

        delay_5m_queue = await channel.declare_queue(
            name="delay_5m_queue",
            durable=True,
            arguments={
                "x-message-ttl": 300000,                        
                "x-dead-letter-exchange": "dispatch_bus",
                "x-dead-letter-routing-key": "dispatch.execute"
            }
        )
        await delay_5m_queue.bind(exchange=delay_bus, routing_key="delay.5m")

        # ---------------------------------------------------------
        # 4. DECLARE THE DEAD LETTER QUEUE (The Fatal Errors)
        # ---------------------------------------------------------
        dlq_queue = await channel.declare_queue("dead_letter_queue", durable=True)
        await dlq_queue.bind(exchange=dlx_bus, routing_key="dlq.fatal")

        print("[v] Successfully provisioned complete Webhook Dispatcher AMQP Topology!")

if __name__ == "__main__":
    asyncio.run(main())