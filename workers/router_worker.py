# workers/router_worker.py
import asyncio
import aio_pika
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

# Import our local modules
from shared.database import AsyncSessionLocal
from shared.models import Endpoint
from shared.schemas import EventMessage, DispatchMessage

RABBITMQ_URL = "amqp://guest:guest@localhost:5672/"

async def main():
    print("Starting Router Worker...")
    
    # 1. Connect to RabbitMQ
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    
    async with connection:
        channel = await connection.channel()
        
        # QOS (Quality of Service): Don't overwhelm the worker. Pull 10 messages at a time.
        await channel.set_qos(prefetch_count=10)
        
        # Connect to our queue and the next exchange in the pipeline
        queue = await channel.get_queue("router_queue")
        dispatch_exchange = await channel.get_exchange("dispatch_bus")

        async def process_message(message: aio_pika.IncomingMessage):
            """This function fires every time a message arrives."""
            
            # Use manual acknowledgments so we control exactly when a message dies
            async with message.process(ignore_processed=True):
                try:
                    # 1. Decode: Bytes -> JSON String -> Pydantic Model
                    event = EventMessage.model_validate_json(message.body.decode())
                    print(f"[*] Processing Event: {event.event_id} | Tenant: {event.tenant_id}")

                    # 2. Query the Database for the URL
                    async with AsyncSessionLocal() as db:
                        # Find the endpoint belonging to this tenant
                        stmt = select(Endpoint).where(Endpoint.tenant_id == event.tenant_id)
                        result = await db.execute(stmt)
                        endpoint_record = result.scalars().first()

                        # If the customer hasn't set up a URL, we drop the message.
                        if not endpoint_record:
                            print(f"[!] No URL found for {event.tenant_id}. Dropping message.")
                            await message.reject(requeue=False) 
                            return

                        # 3. Upgrade the Data Contract
                        dispatch_msg = DispatchMessage(
                            **event.model_dump(),
                            target_url=endpoint_record.target_url
                        )

                    # 4. Push to the Dispatch Bus
                    amqp_msg = aio_pika.Message(
                        body=dispatch_msg.model_dump_json().encode(),
                        content_type="application/json"
                    )
                    await dispatch_exchange.publish(
                        message=amqp_msg,
                        routing_key="dispatch.execute"
                    )

                    # 5. Acknowledge Success
                    await message.ack()
                    print(f"[v] Successfully routed {event.event_id} to {endpoint_record.target_url}")

                except Exception as e:
                    print(f"[X] Worker Error or Poison Message: {e}")
                    # Reject without requeueing. It dies here.
                    await message.reject(requeue=False)

        print("Waiting for messages. To exit press CTRL+C")
        
        # Start consuming!
        await queue.consume(process_message)
        
        # Keep the event loop running forever
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())