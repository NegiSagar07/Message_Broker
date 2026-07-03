import asyncio
import aio_pika

# RabbitMQ local connection string
RABBITMQ_URL = "amqp://guest:guest@localhost:5672/"

async def main():
    print("Connecting to RabbitMQ URL...")
    
    # 1. Open connection
    connection = await aio_pika.connect_robust(RABBITMQ_URL)

    # 2. Use 'async with' to automatically close the connection when the script finishes
    async with connection:
        # Added 'await' here
        channel = await connection.channel()

        # Added 'await' to all exchanges
        raw_event_bus = await channel.declare_exchange("raw_event_bus", aio_pika.ExchangeType.TOPIC, durable=True)
        dispatch_bus = await channel.declare_exchange("dispatch_bus", aio_pika.ExchangeType.DIRECT, durable=True)
        delay_bus = await channel.declare_exchange("delay_bus", aio_pika.ExchangeType.DIRECT, durable=True)

        router_queue = await channel.declare_queue("router_queue", durable=True)
        # Fixed the typo here
        await router_queue.bind(exchange=raw_event_bus, routing_key='#')

        primary_dispatch_queue = await channel.declare_queue("primary_dispatch_queue", durable=True)
        await primary_dispatch_queue.bind(exchange=dispatch_bus, routing_key='dispatch.execute')

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

        terminal_graveyard = await channel.declare_queue(
            name="terminal_graveyard", 
            durable=True
        )
        await terminal_graveyard.bind(exchange=delay_bus, routing_key="delay.terminal")

        print("Successfully provisioned complete Webhook Dispatcher AMQP Topology!")

if __name__ == "__main__":
    asyncio.run(main())